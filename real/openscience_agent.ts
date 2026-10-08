// Один «агент» OpenScience без LLM: поток части пишет их же Storage.write через их
// createCoalescer (250 мс, как в session/index.ts), общая запись — их Storage.upsert
// (чтение-изменение-запись под межпроцессной блокировкой). Копируется в backend/cli/bench-real/ и запускается bun.
//   bun run openscience_agent.ts <agent_id> <duration_s> <out.json>
import { Storage } from "../src/storage/storage"
import { createCoalescer } from "../src/storage/coalescer"

const [id, durS, out] = [process.argv[2], Number(process.argv[3]), process.argv[4]]
const t0 = performance.now()
const partLat: number[] = []
const sharedLat: number[] = []
let partErr = 0, sharedErr = 0, sharedAcked = 0, flushes = 0, lastLen = 0

const writer = createCoalescer<any>(async (_k, part) => {
  const s = performance.now()
  try { await Storage.write(["part", part.messageID, part.id], part); partLat.push(performance.now() - s); flushes++; lastLen = part.text.length }
  catch { partErr++ }
}, 250)

// дельта токенов каждые 20 мс; часть растёт до 64 КБ, потом новая часть
let msg = 0, text = ""
const tick = setInterval(() => {
  text += "x".repeat(40)
  if (text.length > 65536) { msg++; text = "" }
  writer.push(`${id}/${msg}`, { id: `p_${id}_${msg}`, messageID: `m_${id}_${msg}`, sessionID: `s_${id}`, type: "text", text })
}, 20)

// общая запись раз в секунду: счётчик агента в одном файле
const shared = setInterval(async () => {
  const s = performance.now()
  try {
    await Storage.upsert<any>(["bench", "shared"], (d) => ({ ...(d ?? {}), [id]: (d?.[id] ?? 0) + 1 }))
    sharedLat.push(performance.now() - s); sharedAcked++
  } catch { sharedErr++ }
}, 1000)

setTimeout(async () => {
  clearInterval(tick); clearInterval(shared)
  await writer.flushAll().catch(() => {})
  await Bun.sleep(1500)
  await Bun.write(out, JSON.stringify({ id, partLat, sharedLat, partErr, sharedErr, sharedAcked, flushes, lastLen, msg }))
  process.exit(0)
}, durS * 1000)
