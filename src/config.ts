export const NATS_SERVERS = process.env.NATS_SERVERS || 'nats://localhost:4222'
export const NATS_TOKEN = process.env.NATS_TOKEN || ''
export const MIN_BACKOFF_DELAY_SECONDS =
  process.env.MIN_BACKOFF_DELAY_SECONDS || 1
export const MAX_BACKOFF_DELAY_SECONDS =
  process.env.MAX_BACKOFF_DELAY_SECONDS || 32
export const COMPANY_CONFIG = process.env.COMPANY_CONFIG || '{}'
