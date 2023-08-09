import { AttributeValue } from '@aws-sdk/client-dynamodb'
import { ux } from '@oclif/core'
import * as clc from 'cli-color'
import { Dirent, readdirSync } from 'node:fs'
import * as path from 'node:path'
import { BaseCommand } from '../../base-command'
import { buildUpdateProps } from '../../libs/dynamodb'
import { TransformationRecord, TransformationRecordSchema } from '../../types'
import PQueue from 'p-queue-cjs';
import { deepDiff } from '../../libs/deepDiff'
import _ = require('lodash')

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

    const path = [this.transformationsDirectory, transformationDir.name].join('/')

    const { Transform } = await import(path)
    const transformation = new Transform({ ddb: this.ddb })

    const stats = {
      total: 0,
      updated: 0,
      deleted: 0,
      skipped: 0,
      noChange: 0,
    }

    const BATCH_WRITE_CONCURRENCY = 4
    const processScanPageQueue = new PQueue({ concurrency: 4 })
    const batchWriteQueue = new PQueue({ concurrency: BATCH_WRITE_CONCURRENCY })

    const scanParams = transformation.scanParams ? transformation.scanParams() : {}
    let ExclusiveStartKey: Record<string, AttributeValue> | undefined
    do {
      const result = await this.ddb.scan({
        ...scanParams,
        TableName: Transform.TableName,
        ExclusiveStartKey,
      })

      ExclusiveStartKey = result.LastEvaluatedKey

      processScanPageQueue.add(async () => {
        let skippedThisPage = 0
        let noChangeThisPage = 0
        let updatedThisPage = 0
        let deletedThisPage = 0

        this.log(`scan returned batch of ${result?.Items?.length || 0} records`)
        if (!result.Items) return

        stats.total += result.Items.length

        const nonSkipped: Record<string, any>[] = []

        if (transformation.serializeSkip === true) {
          for await (const item of result.Items) {
            const skip = await transformation.skip(item)
            if (!skip) nonSkipped.push(item)
          }
        } else {
          const skipResults = await Promise.all(result.Items.map(async item => {
            if (await transformation.skip(item)) return null
            return item
          }))
          skipResults.forEach(item => item && nonSkipped.push(item))
        }

        skippedThisPage += result.Items.length - nonSkipped.length

        const transformSingle = async (item: Record<string, any>): Promise<null | { put: any, delete: boolean, sourceKeys: Record<string, any>, diff: any }> => {
          const transformed = await transformation.forward(item)

          const sourceKeys = Transform.KeyNames.reduce(
            (acc: Record<string, AttributeValue>, key: string) => ({ ...acc, [key]: item[key] }),
            {}
          )

          if (transformed === null) {
            // if `forward` returns explicitly `null`, we delete the record
            return {
              put: null,
              delete: true,
              sourceKeys,
              diff: null,
            }
          } else if (!transformed) {
            // if `forward` returns `undefined`/falsy, we skip the record
            return null
          }

          if (_.isEqual(item, transformed)) {
            noChangeThisPage++ // FIXME: the var scoping for this is whack
            return null
          }

          let diff = null
          try {
            diff = deepDiff(item, transformed)
          } catch (err: any) {
            //
            console.log(`diff error`, err)
          }

          const keysUpdated = Object.keys(sourceKeys).reduce((_acc, key) => item[key] !== transformed[key], false)

          return {
            put: transformed,
            delete: keysUpdated,
            sourceKeys,
            diff,
          }
        }
        const results: Array<null | { put: any, delete: boolean, sourceKeys: Record<string, any>, diff: any }> = []
        if (transformation.serializeForward === true) {
          for await (const item of nonSkipped) {
            const result = await transformSingle(item)

            results.push(result)
          }
        } else {
          const transformResults = await Promise.all(nonSkipped.map(async item => transformSingle(item)))
          results.push(...transformResults)
        }

        const actions: Record<string, any>[] = []
        results.forEach(result => {
          if (result === null) return

          if (result?.put) {
            actions.push({
              PutRequest: {
                Item: result.put,
              },
            })
          }

          if (result?.delete === true && result.sourceKeys) {
            actions.push({
              DeleteRequest: {
                Key: result.sourceKeys,
              },
            })
            deletedThisPage++
          }

          this.log(`update diff for ${Object.entries(result.sourceKeys).map(([k, v]) => `${k}:${v}`).join(' ')} ${result.delete ? '(deleted)' : ''}: ${JSON.stringify(result.diff)}`)

          updatedThisPage++
        })

        const processBatch = async (batchOfActions: Record<string, any>[], attempt: number = 1) => {
          this.log(`sending batchWrite request with ${batchOfActions.length} items (attempt ${attempt})`)
          try {
            const response = await this.ddb.batchWrite({
              RequestItems: {
                [Transform.TableName]: batchOfActions
              },
            })
            if (response.UnprocessedItems && response.UnprocessedItems[Transform.TableName]) {
              const retryBatch = response.UnprocessedItems[Transform.TableName]
              this.log(`re-enqueuing ${retryBatch.length} unprocessed items`)
              await batchWriteQueue.add(() => processBatch(retryBatch, attempt++))
            } else {
              const currentConcurrency = batchWriteQueue.concurrency
              if (currentConcurrency < BATCH_WRITE_CONCURRENCY) {
                const newConcurrency = currentConcurrency + 1
                batchWriteQueue.concurrency = newConcurrency
                this.log(`Increased batchWrite concurrency to ${newConcurrency}`)
              }
            }
          } catch (err: any) {
            if (err.name === 'ThrottlingException') {
              console.error(`Received ThrottlingException, pausing queue for 1 minute`)
              if (!batchWriteQueue.isPaused) {
                batchWriteQueue.pause()
                await batchWriteQueue.add(() => processBatch(batchOfActions, attempt++))
                setTimeout(() => {
                  if (batchWriteQueue.isPaused) {
                    console.log(`Resuming queue, with c=1, after 1 minute delay due to a ThrottlingException`)
                    batchWriteQueue.concurrency = 1
                    batchWriteQueue.start()
                  }
                }, 60 * 1000)
              }
            } else {
              console.error(`batchWrite error`)
              console.error(err)
              throw err
            }
          }
        }

        while (actions.length > 0) {
          const batch = actions.splice(0, 25)
          batchWriteQueue.add(() => processBatch(batch))
        }
        this.log(`skipped ${skippedThisPage}, no change ${noChangeThisPage}, updated ${updatedThisPage}, deleted ${deletedThisPage} records in batch`)
        stats.skipped += skippedThisPage
        stats.updated += updatedThisPage
        stats.deleted += deletedThisPage
        stats.noChange += noChangeThisPage
      })
    } while (ExclusiveStartKey && await new Promise(async resolve => {
      if (batchWriteQueue.size >= 200) {
        this.log(`waiting for batchWrite queue to drain (size: ${batchWriteQueue.size})`)
        await batchWriteQueue.onSizeLessThan(200)
        this.log(`batchWrite queue sufficiently drained, continuing (size: ${batchWriteQueue.size})`)
      }
      resolve(true)
    }) && await new Promise(async resolve => {
      if (processScanPageQueue.size >= 20) {
        this.log(`waiting for processScanPage queue to drain (size: ${processScanPageQueue.size})`)
        await processScanPageQueue.onSizeLessThan(20)
        this.log(`processScanPage queue sufficiently drained, continuing dynamodb pagination (size: ${processScanPageQueue.size})`)
      }
      resolve(true)
    }))

    this.log(`dynamodb scan completed, waiting for queue to drain`)
    await batchWriteQueue.onIdle()

    await new Promise(resolve => {
      setTimeout(resolve, 5000)
    })

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
