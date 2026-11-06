/**
 * The prompt the page's "Copy prompt" button hands to a visitor's coding
 * agent. Every fact in it comes from the deployment (clone URL, API base,
 * canvas size, tiles per push), so it stays true when they change.
 */
export interface PromptInputs {
  /** The page, e.g. `https://place.example.com`. */
  pageUrl: string;
  /** `git clone` URL of the canvas repository. */
  cloneUrl: string;
  /** Absolute API base, e.g. `https://place.example.com/api/v1`. */
  apiUrl: string;
  width: number;
  height: number;
  tilesPerPush: number;
}

export function agentPrompt(p: PromptInputs): string {
  const dir =
    p.cloneUrl
      .replace(/\/+$/, '')
      .split('/')
      .pop()
      ?.replace(/\.git$/, '') || 'canvas';
  const n = p.tilesPerPush;
  const spot =
    p.width === p.height ? `x, y from 0 to ${p.width - 1}` : `x from 0 to ${p.width - 1}, y from 0 to ${p.height - 1}`;
  return [
    `Help me paint something on the shared pixel canvas at ${p.pageUrl}. Every pixel is a real git push to an Aspen git server. Start by asking me what to paint and where (step 3), and don't paint anything until I say go.`,
    '',
    `1. git clone --depth=1 ${p.cloneUrl} && cd ${dir}`,
    `2. Read README.md for the rules: each tile is tiles/<x>_<y>.txt (x from 0 to ${p.width - 1}, y from 0 to ${p.height - 1}), holding one palette color as lowercase #rrggbb plus a newline. Use only palette colors (GET ${p.apiUrl}/config lists them) and only tile files. At most ${n} tiles per push.`,
    '3. Before painting, ask me two things and wait for my answers:',
    '   - What should we paint? (an icon, a word, a pattern, a flag…)',
    `   - Where on the canvas? Offer three choices: a spot I name (${spot}), an area you find without anyone's recognizable picture, or "surprise me".`,
    `   To suggest a spot, look at the current picture first (GET ${p.apiUrl}/canvas returns "pixels", ${p.width} x ${p.height} characters in row-major order, index y * ${p.width} + x, each "." for a tile never painted or the tile's index into the config's palette in base 36), and tell me if the area I chose would paint over someone else's work.`,
    `4. Show me a small preview of the plan (a grid of the tiles and colors) and the exact coordinates. Paint only after I say go: write the tile files, for example printf '#d6262b\\n' > tiles/12_40.txt, at most ${n} tiles per push.`,
    '5. git add tiles && git commit -m "paint: <what you drew>" && git push origin HEAD:refs/heads/paint/<a-short-name>',
    `6. For a bigger picture, repeat in pushes of up to ${n} tiles, no more than one push every few seconds.`,
    `7. Open ${p.pageUrl} and confirm your picture appears.`,
    '',
    "Your commit's name and email are public. Use a pseudonym if you prefer: git -c user.name=... -c user.email=... commit ...",
    '',
  ].join('\n');
}
