/**
 * A small `text/event-stream` parser (WHATWG HTML, "server-sent events").
 * The client reads the stream with `fetch` instead of `EventSource` so it can
 * resume from an exact sequence number and works the same in Node tests.
 */
export interface SseMessage {
  /** The `event:` field; `message` when absent. */
  event: string;
  data: string;
  /** The last `id:` seen on the stream so far, or null. */
  id: string | null;
}

export interface SseParser {
  /** Feeds decoded text; calls `onMessage` for each complete event. */
  push(chunk: string): void;
  /** Reconnection delay the server asked for with `retry:`, if any. */
  readonly retryMs: number | null;
}

export function createSseParser(onMessage: (m: SseMessage) => void): SseParser {
  let buffer = '';
  let event = '';
  let data: string[] = [];
  let lastId: string | null = null;
  let retryMs: number | null = null;
  let first = true;

  const dispatch = () => {
    if (data.length > 0) onMessage({ event: event || 'message', data: data.join('\n'), id: lastId });
    event = '';
    data = [];
  };

  const line = (l: string) => {
    if (l === '') return dispatch();
    if (l.startsWith(':')) return;
    const colon = l.indexOf(':');
    const field = colon < 0 ? l : l.slice(0, colon);
    let value = colon < 0 ? '' : l.slice(colon + 1);
    if (value.startsWith(' ')) value = value.slice(1);
    switch (field) {
      case 'event':
        event = value;
        break;
      case 'data':
        data.push(value);
        break;
      case 'id':
        if (!value.includes('\0')) lastId = value;
        break;
      case 'retry':
        if (/^[0-9]+$/.test(value)) retryMs = Number(value);
        break;
    }
  };

  return {
    push(chunk: string) {
      buffer += chunk;
      if (first && buffer.length > 0) {
        if (buffer.charCodeAt(0) === 0xfeff) buffer = buffer.slice(1);
        first = false;
      }
      for (;;) {
        const m = /\r\n|\r|\n/.exec(buffer);
        if (!m) break;
        // A lone CR at the end of a chunk may be the first half of CRLF.
        if (m[0] === '\r' && m.index === buffer.length - 1) break;
        line(buffer.slice(0, m.index));
        buffer = buffer.slice(m.index + m[0].length);
      }
    },
    get retryMs() {
      return retryMs;
    },
  };
}
