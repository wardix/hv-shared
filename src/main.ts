import { connect, headers } from 'nats'
import {
  NATS_SERVERS,
  NATS_TOKEN,
  MIN_BACKOFF_DELAY_SECONDS,
  MAX_BACKOFF_DELAY_SECONDS,
  COMPANY_CONFIG,
} from './config'

const parsedCompanyConfig = JSON.parse(COMPANY_CONFIG)

async function consumeMessages() {
  let backoffDelay = Number(MIN_BACKOFF_DELAY_SECONDS) * 1000
  process.on('SIGINT', async () => {
    await nc.drain()
    process.exit()
  })

  const nc = await connect({
    servers: NATS_SERVERS,
    token: NATS_TOKEN,
  })

  const js = nc.jetstream()
  const c = await js.consumers.get('EVENTS', 'hikvision_shared_processor')

  try {
    while (true) {
      const messages = await c.fetch({ max_messages: 8, expires: 1000 })
      let hasMessages = false
      for await (const message of messages) {
        hasMessages = true
        const forwardedHeaders = headers()
        for (const header of message.headers!.keys()) {
          forwardedHeaders.set(header, message.headers!.get(header))
        }
        const companyId = forwardedHeaders.get('Company-ID')
        if (companyId in parsedCompanyConfig) {
          for (const id of parsedCompanyConfig[companyId]) {
            forwardedHeaders.set('Company-ID', id)
            await js.publish('hikvision_access_verified', message.data, {
              headers: forwardedHeaders,
            })
          }
        } else {
          await js.publish('hikvision_access_verified', message.data, {
            headers: forwardedHeaders,
          })
        }

        message.ack()
        backoffDelay = 1000
      }
      if (!hasMessages) {
        await new Promise((resolve) => setTimeout(resolve, backoffDelay))
        backoffDelay = Math.min(
          backoffDelay * 2,
          Number(MAX_BACKOFF_DELAY_SECONDS) * 1000,
        )
      }
    }
  } catch (error) {
    console.error('Error during message consumtion: ', error)
  }
}

consumeMessages().catch((error) => {
  console.error('Error consuming messages:', error)
})
