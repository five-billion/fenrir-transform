import { CreateTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { BaseCommand } from '../../base-command.js'

const client = new DynamoDBClient({})

export default class Init extends BaseCommand<typeof Init> {
  static description = 'Initialize transformations project'
  static flags = {}
  static args = {}

  async run(): Promise<void> {
    const command = {
      TableName: this.transformationRecordsTableName,
      AttributeDefinitions: [
        {
          AttributeName: 'transformationId',
          AttributeType: 'S',
        },
      ],
      KeySchema: [
        {
          AttributeName: 'transformationId',
          KeyType: 'HASH',
        },
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1,
      },
    }

    const response = await client.send(new CreateTableCommand(command))
    this.log('Created table:', this.transformationRecordsTableName)
  }
}
