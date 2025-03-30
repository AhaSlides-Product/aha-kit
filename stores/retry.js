const getBackoffDelay = ({ baseDelayMs, maxTimeMs, attempt }) => {
  const jitter = Math.random() * baseDelayMs
  return Math.min(
    baseDelayMs * Math.pow(2, attempt) + jitter,
    maxTimeMs,
  )
}

const tryWithBackoffRetry = async ({
  funcWoArgs,
  maxTimeMs,
  baseDelayMs = 100,
  allowedErrorType = undefined,
}) => {
  const startTime = Date.now()

  let attempt = 0
  while (true) {
    try {
      const result = await funcWoArgs()
      return result
    } catch (error) {
      if (allowedErrorType && !(error instanceof allowedErrorType)) {
        throw error
      }

      const elapsedTime = Date.now() - startTime;
      if (elapsedTime >= maxTimeMs) {
        throw error
      }
      const delay = getBackoffDelay({
        baseDelayMs,
        maxTimeMs: maxTimeMs - elapsedTime,
        attempt,
      })

      await new Promise((resolve) => setTimeout(resolve, delay));
      attempt++;
    }
  }
}

module.exports = {
  tryWithBackoffRetry,
}
