import {
  DynamoDBClient
} from '@aws-sdk/client-dynamodb'
import { DynamoDBDocument } from '@aws-sdk/lib-dynamodb'
import { SerializeMiddleware } from '@aws-sdk/types'
import * as DDBUtl from '@aws-sdk/util-dynamodb'
import { ExpressionAttributes, UpdateExpression } from '@aws/dynamodb-expressions'

export type DynamoDBRecord = Record<string, DDBUtl.NativeAttributeValue>

export const SimplifiedDDBDocument = (marshall?: (args: any) => any, unmarshall?: (args: any) => any) => {
  const marshallOptions = {
    // Whether to automatically convert empty strings, blobs, and sets to `null`.
    convertEmptyValues: false, // false, by default.
    removeUndefinedValues: true,
    // Whether to convert typeof object to map attribute.
    convertClassInstanceToMap: false, // false, by default.
  }

  const unmarshallOptions = {
    // Whether to return numbers as a string instead of converting them to native JavaScript numbers.
    wrapNumbers: false, // false, by default.
  }

  const translateConfig = { marshallOptions, unmarshallOptions }

  const ddbClient = new DynamoDBClient({ region: process.env.AWS_REGION })

  const ddb = DynamoDBDocument.from(ddbClient, translateConfig)

  const normalizerMiddleware: SerializeMiddleware<any, any> = next => async args => {
    if (!args.input.Item) return next(args)

    if (marshall) args.input.Item = marshall(args.input.Item)

    return next(args)
  }

  ddb.middlewareStack.addRelativeTo(normalizerMiddleware, {
    relation: 'before',
    toMiddleware: 'DocumentMarshall',
  })

  return ddb
}

export const buildUpdateProps = (item: DynamoDBRecord) => {
  const expression = new UpdateExpression()
  const attributes = new ExpressionAttributes()

  Object.entries(item).forEach(([key, value]) => {
    expression.set(key, value)
  })

  return {
    UpdateExpression: expression.serialize(attributes),
    ExpressionAttributeNames: attributes.names,
    ExpressionAttributeValues: DDBUtl.unmarshall(attributes.values as any),
  }
}
