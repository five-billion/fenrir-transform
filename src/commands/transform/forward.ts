import { AttributeValue } from '@aws-sdk/client-dynamodb'
import { ux } from '@oclif/core'
import clc from 'cli-color'
import { Dirent, readdirSync } from 'node:fs'
import * as path from 'node:path'
import PQueue from 'p-queue'
import { BaseCommand } from '../../base-command.js'
import { buildUpdateProps } from '../../libs/dynamodb.js'
import { TransformationRecord, TransformationRecordSchema } from '../../types.js'

type TransformationRecordKeyBatch = { transformationId: string }[]
type TransformationState = { transformation: Dirent; record?: TransformationRecord }
type TransformationStateSet = Record<string, TransformationState>

const backoff = async (attempt: number, maxAttempts: number, jitter = 0.5) =>
  new Promise((resolve, reject) => {
    if (attempt >= maxAttempts) reject('Max attempts exceeded.')
    setTimeout(resolve, 2 ** attempt * 1000 * (1 + Math.random() * jitter))
  })

export default class Transform extends BaseCommand<typeof Transform> {
  static description = 'Run data transformations'
  static flags = {}
  static args = {}

  get transformationsDirectory(): string {
    return path.join(process.cwd(), 'transformations')
  }

  allTransformations(): Dirent[] {
    const contents = readdirSync(this.transformationsDirectory, { withFileTypes: true })
    return contents.filter(f => f.isDirectory())
  }

  async setLockedRecord(id: string): Promise<void> {
    await this.ddb.put({
      TableName: this.transformationRecordsTableName,
      Item: {
        transformationId: id,
        status: 'LOCKED',
        lockId: 'TEST',
        lockedAt: new Date().toISOString(),
        lockedBy: 'TEST',
        lockMessage: 'LOCK TEST',
        startedAt: new Date().toISOString(),
      },
    })
  }

  async setCompletedRecord(id: string): Promise<void> {
    await this.ddb.update({
      TableName: this.transformationRecordsTableName,
      Key: {
        transformationId: id,
      },
      ...buildUpdateProps({
        status: 'COMPLETED',
        completedAt: new Date().toISOString(),
      }),
    })
  }

  async runTransformation(transformationDir: Dirent): Promise<void> {
    this.log('Running transformation:', transformationDir.name)

    await this.setLockedRecord(transformationDir.name)

    const path = [this.transformationsDirectory, transformationDir.name, 'index.ts'].join('/')

    const { Transform } = await import(path)
    const transformation = new Transform({ ddb: this.ddb })

    const stats = {
      total: 0,
      updated: 0,
      skipped: 0,
      page: 0
    }

    const queue = new PQueue({ concurrency: 5 })

    let ExclusiveStartKey: Record<string, AttributeValue> | undefined

    do {
      const result = await this.ddb.scan({
        TableName: Transform.TableName,
        ExclusiveStartKey,
      })

      ExclusiveStartKey = result.LastEvaluatedKey

      this.log(`scan returned ${result?.Items?.length || 0} records on page ${stats.page}`)

      if (result.Items) {
        stats.total += result.Items.length

        for (const item of result.Items) {
          const skipped = (await transformation?.skip(item)) || false
          if (skipped) {
            stats.skipped++
            continue
          }

          await queue.add(async () => {
            const transformed = await transformation.forward(item)

            if (transformed) {
              const sourceKeys = Transform.KeyNames.reduce(
                (acc: Record<string, AttributeValue>, key: string) => ({ ...acc, [key]: item[key] }),
                {}
              )

              const keysUpdated = Object.keys(sourceKeys).reduce((acc, key) => item[key] !== transformed[key], false)

              const actions: Record<string, any>[] = [
                {
                  PutRequest: {
                    Item: transformed,
                  },
                },
              ]

              if (keysUpdated) {
                actions.push({
                  DeleteRequest: {
                    Key: sourceKeys,
                  },
                })
              }

              console.log({
                RequestItems: {
                  [Transform.TableName]: actions,
                },
              })

              await this.ddb.batchWrite({
                RequestItems: {
                  [Transform.TableName]: actions,
                },
              })

              this.log('action:', actions)

              stats.updated++
            }

            stats.total++
          })
        }
      }
      stats.page++
    } while (ExclusiveStartKey && await new Promise(async resolve => {
      await queue.onSizeLessThan(500)
      resolve(true)
    }))

    console.log(`dynamodb scan finished, waiting for queue to drain`)
    await queue.onIdle()

    await this.setCompletedRecord(transformationDir.name)

    this.log(`transformation complete ${JSON.stringify(stats, null, 2)}`)
  }

  async transformationState() {
    const transformations = this.allTransformations()

    const batches = transformations.reduce((acc, item, index) => {
      const chunkIndex = Math.floor(index / 100)

      if (!acc[chunkIndex]) acc[chunkIndex] = []

      acc[chunkIndex].push({ transformationId: item.name })

      return acc
    }, [] as TransformationRecordKeyBatch[])

    const records: TransformationRecord[] = []

    for (const batch of batches) {
      let attempts = 0
      let Keys = [...batch]

      while (Keys.length > 0) {
        const response = await this.ddb.batchGet({
          RequestItems: { [this.transformationRecordsTableName]: { Keys } },
        })

        Keys =
          (response.UnprocessedKeys?.[this.transformationRecordsTableName]?.Keys as TransformationRecordKeyBatch) || []

        records.push(
          ...(response.Responses || {})[this.transformationRecordsTableName].map(r =>
            TransformationRecordSchema.parse(r)
          )
        )

        if (Keys.length > 0) await backoff(attempts++, 5)
      }
    }

    return transformations.reduce((acc, t) => {
      const record = records.find(r => r.transformationId === t.name)
      return { ...acc, [t.name]: { transformation: t, record } as TransformationState }
    }, {} as TransformationStateSet)
  }

  displayState(transformationState: TransformationStateSet) {
    ux.table<TransformationState>(Object.values(transformationState), {
      id: {
        header: 'id',
        get: (t: TransformationState) => t.transformation.name,
      },
      startedAt: {
        header: 'started at',
        get: (t: TransformationState) => t.record?.startedAt,
      },
      status: {
        header: 'status',
        get: (t: TransformationState) => {
          switch (t.record?.status) {
            case 'LOCKED':
              return clc.bgYellow('LOCKED')
            case 'ERROR':
              return clc.red('ERROR')
            case 'COMPLETED':
              return clc.green('COMPLETED')
            case undefined:
              return clc.blue('PENDING')
            default:
              return clc.bgRed('UNKNOWN')
          }
        },
      },
    })
  }

  async run(): Promise<void> {
    this.log('Transforming data...')
    const transformationState = await this.transformationState()

    this.displayState(transformationState)

    const unstable = Object.values(transformationState).filter(
      state => ![undefined, 'COMPLETED'].includes(state.record?.status)
    )

    if (unstable.length > 0) {
      this.error(
        `Cannot run transformations while the following transformations are in unstable states: ${unstable
          .map(t => `${t.transformation.name} (${t.record?.status})`)
          .join(', ')}`
      )
    }

    for (const [path, state] of Object.entries(transformationState)) {
      if (state.record) {
        if (state.record.status !== 'COMPLETED') throw new Error(`Unexpected state: ${state.record.status}`)

        this.log(`Skipping ${path} because it has already completed.`)
        continue
      }

      await this.runTransformation(state.transformation)
    }
  }
}
