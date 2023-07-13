import * as z from 'zod'

export const LogLevelSchema = z.enum(['debug', 'info', 'warn', 'error'])

export const ProjectConfigSchema = z.object({
  name: z.string(),
  logLevel: LogLevelSchema.optional(),
})

export type LogLevel = z.infer<typeof LogLevelSchema>
export type ProjectConfig = z.infer<typeof ProjectConfigSchema>

export const TransformationRecordSchemaBase = z.object({
  transformationId: z.string(),
  status: z.enum(['LOCKED', 'COMPLETED', 'ERROR']),
  startedAt: z.string().datetime(),
})

export const LockedTransformationRecordSchema = TransformationRecordSchemaBase.extend({
  status: z.literal('LOCKED'),
  lockId: z.string(),
  lockedAt: z.string().datetime(),
  lockedBy: z.string(),
  lockMessage: z.string().optional(),
})

export const CompletedTransformationRecordSchema = TransformationRecordSchemaBase.extend({
  status: z.literal('COMPLETED'),
  completedAt: z.string().datetime(),
  results: z.any().optional(),
})

export const ErrorTransformationRecordSchema = TransformationRecordSchemaBase.extend({
  status: z.literal('ERROR'),
  errorAt: z.string().datetime(),
  errorMessage: z.string(),
})

export const TransformationRecordSchema = z.discriminatedUnion('status', [
  LockedTransformationRecordSchema,
  CompletedTransformationRecordSchema,
  ErrorTransformationRecordSchema,
])

export type TransformationRecord = z.infer<typeof TransformationRecordSchema>
