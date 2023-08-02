import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
import { Command, Flags, Interfaces } from '@oclif/core'
import * as fs from 'fs'
import * as path from 'node:path'
import { SimplifiedDDBDocument } from './libs/dynamodb.js'
import { LogLevel, LogLevelSchema, ProjectConfig, ProjectConfigSchema } from './types.js'

export type Flags<T extends typeof Command> = Interfaces.InferredFlags<(typeof BaseCommand)['baseFlags'] & T['flags']>
export type Args<T extends typeof Command> = Interfaces.InferredArgs<T['args']>

export abstract class BaseCommand<T extends typeof Command> extends Command {
  static baseFlags = {
    'log-level': Flags.custom<LogLevel>({
      summary: 'Specify level for logging.',
      options: Object.values(LogLevelSchema._def.values),
      helpGroup: 'GLOBAL',
    })(),
  }

  protected projectConfig!: ProjectConfig
  protected flags!: Flags<T>
  protected args!: Args<T>

  protected async loadProjectConfig(): Promise<ProjectConfig> {
    const pathSegments = process.cwd().split(path.sep)

    while (pathSegments.length > 0) {
      const configPath = [...pathSegments, 'fenrirconfig.json'].join(path.sep)
      if (!fs.existsSync(configPath)) {
        pathSegments.pop()
        continue
      }

      this.log('Loading project config from:', configPath)
      const json = fs.readFileSync(configPath, { encoding: 'utf-8' })
      const config = JSON.parse(json)
      return ProjectConfigSchema.parse(config)
    }

    throw new Error('Could not find project config.')
  }

  get ddb(): DynamoDBDocument {
    return SimplifiedDDBDocument()
  }

  get transformationRecordsTableName(): string {
    return `DataTransformations-${this.projectConfig.name}`
  }

  public async init(): Promise<void> {
    await super.init()

    this.projectConfig = await this.loadProjectConfig()
    this.log('Project config:', this.projectConfig)

    const { args, flags } = await this.parse({
      flags: this.ctor.flags,
      baseFlags: (super.ctor as typeof BaseCommand).baseFlags,
      args: this.ctor.args,
      strict: this.ctor.strict,
    })
    this.flags = flags as Flags<T>
    this.args = args as Args<T>
  }
}
