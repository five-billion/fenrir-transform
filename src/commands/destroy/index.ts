import { CreateTableCommand, DeleteTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { BaseCommand } from '../../base-command'

const client = new DynamoDBClient({})

export default class Init extends BaseCommand<typeof Init> {
  static description = 'Initialize transformations project'
  static flags = {}
  static args = {}

  async run(): Promise<void> {
    const response = await client.send(new DeleteTableCommand({ TableName: this.transformationRecordsTableName }))
    this.log('Deleted table:', this.transformationRecordsTableName)
  }
}
