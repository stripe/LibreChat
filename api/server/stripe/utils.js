const { logger } = require('@librechat/data-schemas');
const UNREDACTED_HEADERS = process.env.UNREDACTED_HEADERS;

let unredactedHeaders = [];
if (UNREDACTED_HEADERS) {
  unredactedHeaders = UNREDACTED_HEADERS.split(',')
    .map(name => name.trim().toLowerCase())
    .filter(name => name.length > 0);
}

function redactHeader(headerName, headerValue) {
  if (!headerName || !headerValue) return '';
  
  if (unredactedHeaders.length > 0) {
    if (unredactedHeaders.includes(headerName.toLowerCase())) {
      return headerValue;
    }
  } else {
    logger.info('[Stripe:redactHeader] No unredacted headers found, redacting header');
  }
  
  return `[REDACTED ${headerValue.length} CHARACTERS]`;
}

module.exports = {
  redactHeader,
};