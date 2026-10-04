/**
 * Ajuste da Baileys dentro da imagem da Evolution: repassar o HISTÓRICO DE
 * CHAMADAS do WhatsApp como evento `call` (webhook CALL da Evolution).
 *
 * Por que existe (02/10/2026, pedido do MATHEUS): ligação feita PELO CELULAR
 * do vendedor não manda sinal de chamada ao aparelho conectado (a Evolution).
 * Ela chega só pela sincronização do histórico de chamadas, como
 * `SyncActionValue.callLogAction.callLogRecord` (resultado, duração em
 * segundos, início em segundos Unix, se foi recebida, participantes). A
 * Baileys decodifica esse registro e o JOGA FORA: `processSyncAction` não tem
 * o caso `callLogAction` (7.0.0-rc.9 e a master de 02/10/2026). Este ajuste
 * repassa o registro sem mexer em mais nada.
 *
 * O que o ajuste faz, e só isso:
 *  1. Insere a função `klozzerRepassarCallLog` antes de `makeChatsSocket`, em
 *     `lib/Socket/chats.js`.
 *  2. Chama a função logo depois de `processSyncAction` em
 *     `newAppStateChunkHandler`, o único lugar em que o tradutor de LID para
 *     telefone (`signalRepository.lidMapping`) está ao alcance. O outro lado da
 *     ligação chega como LID; sem o telefone, o Klozzer não acharia a conversa.
 *
 * Regras que importam num servidor que atende TODAS as organizações:
 *  - nada aqui pode quebrar a sincronização: tudo em try/catch, e a tradução de
 *    LID roda fora do caminho síncrono (falha vira log, nunca exceção);
 *  - o evento sai com `status: 'call_log'`, nunca 'offer': a Evolution só
 *    recusa chamada ou responde automaticamente quando o status é 'offer';
 *  - registro mais velho que KLOZZER_CALL_LOG_MAX_AGE_HOURS (padrão 48) não sai:
 *    na primeira sincronização depois de parear, o histórico INTEIRO de
 *    chamadas passa por aqui, e virar centenas de webhooks não ajuda ninguém;
 *  - o build falha se a Baileys não for a versão conferida, se uma âncora não
 *    aparecer exatamente uma vez, ou se o arquivo resultante não carregar.
 *
 * Uso (no Dockerfile): node patch-chamadas.mjs /evolution/node_modules/baileys
 */

import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

export const VERSAO_CONFERIDA = "7.0.0-rc.9";
export const MARCA = "/* klozzer:call-log v1 */";

export const ANCORA_FUNCAO = "export const makeChatsSocket = (config) => {";
export const ANCORA_CHAMADA =
  "processSyncAction(mutation, ev, authState.creds.me, isInitialSync ? { accountSettings: authState.creds.accountSettings } : undefined, logger);";

const FUNCAO = `${MARCA}
const klozzerNumero = (v) => {
    if (v === null || v === undefined) return null;
    if (typeof v === 'object' && typeof v.toNumber === 'function') return v.toNumber();
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
};
const klozzerNomeEnum = (en, v) => {
    if (v === null || v === undefined) return null;
    if (typeof v === 'string') return v;
    const nome = en ? en[v] : undefined;
    return typeof nome === 'string' ? nome : String(v);
};
const klozzerRepassarCallLog = (mutation, ev, me, isInitialSync, signalRepository, logger) => {
    try {
        const r = mutation && mutation.syncAction && mutation.syncAction.value && mutation.syncAction.value.callLogAction
            ? mutation.syncAction.value.callLogAction.callLogRecord
            : null;
        if (!r) return;
        const inicioBruto = klozzerNumero(r.startTime);
        const inicioMs = inicioBruto === null ? null : (inicioBruto > 1e12 ? inicioBruto : inicioBruto * 1000);
        const idadeMaxH = Number(process.env.KLOZZER_CALL_LOG_MAX_AGE_HOURS || 48);
        if (inicioMs !== null && Number.isFinite(idadeMaxH) && Date.now() - inicioMs > idadeMaxH * 3600 * 1000) return;
        const enums = proto && proto.CallLogRecord ? proto.CallLogRecord : {};
        const telefoneDe = async (jid) => {
            if (typeof jid !== 'string' || !jid.endsWith('@lid')) return null;
            try {
                const lm = signalRepository && signalRepository.lidMapping;
                return lm && typeof lm.getPNForLID === 'function' ? (await lm.getPNForLID(jid)) || null : null;
            } catch (e) {
                return null;
            }
        };
        (async () => {
            const participantes = [];
            for (const p of r.participants || []) {
                const jid = p && p.userJid ? String(p.userJid) : null;
                participantes.push({ jid, pn: await telefoneDe(jid), callResult: klozzerNomeEnum(enums.CallResult, p ? p.callResult : null) });
            }
            const criador = r.callCreatorJid ? String(r.callCreatorJid) : null;
            ev.emit('call', [{
                id: r.callId || null,
                from: criador,
                chatId: participantes.length ? participantes[0].jid : criador,
                date: new Date(inicioMs !== null ? inicioMs : Date.now()),
                offline: false,
                status: 'call_log',
                isVideo: !!r.isVideo,
                isGroup: !!r.groupJid,
                groupJid: r.groupJid || undefined,
                klozzerCallLog: {
                    versao: 1,
                    callId: r.callId || null,
                    callCreatorJid: criador,
                    callCreatorPn: await telefoneDe(criador),
                    isIncoming: r.isIncoming === null || r.isIncoming === undefined ? null : !!r.isIncoming,
                    isVideo: !!r.isVideo,
                    isCallLink: !!r.isCallLink,
                    isDndMode: !!r.isDndMode,
                    callResult: klozzerNomeEnum(enums.CallResult, r.callResult),
                    callType: klozzerNomeEnum(enums.CallType, r.callType),
                    silenceReason: klozzerNomeEnum(enums.SilenceReason, r.silenceReason),
                    durationSeconds: klozzerNumero(r.duration),
                    startTime: inicioBruto,
                    groupJid: r.groupJid || null,
                    participants: participantes,
                    me: { id: me && me.id ? me.id : null, lid: me && me.lid ? me.lid : null },
                    initialSync: !!isInitialSync,
                    index: Array.isArray(mutation.index) ? mutation.index.map(String) : [],
                },
            }]);
        })().catch((e) => {
            logger && logger.warn && logger.warn({ err: String(e) }, 'klozzer call log: falha ao repassar');
        });
    } catch (e) {
        logger && logger.warn && logger.warn({ err: String(e) }, 'klozzer call log: falha ao ler o registro');
    }
};
`;

/** O código inserido, exposto só para o teste exercitar a função de verdade. */
export const CODIGO_DA_FUNCAO = FUNCAO;

function contar(fonte, trecho) {
  let n = 0;
  let i = fonte.indexOf(trecho);
  while (i !== -1) {
    n += 1;
    i = fonte.indexOf(trecho, i + trecho.length);
  }
  return n;
}

/**
 * Aplica o ajuste ao texto de `lib/Socket/chats.js`. Puro: devolve o texto novo,
 * o mesmo texto se já estiver ajustado, ou LANÇA quando uma âncora não aparece
 * exatamente uma vez (Baileys diferente da conferida não recebe ajuste às cegas).
 */
export function aplicarPatchNoChats(fonte) {
  if (fonte.includes(MARCA)) return fonte;
  const nFuncao = contar(fonte, ANCORA_FUNCAO);
  const nChamada = contar(fonte, ANCORA_CHAMADA);
  if (nFuncao !== 1 || nChamada !== 1) {
    throw new Error(
      `âncora fora do esperado em chats.js (makeChatsSocket: ${nFuncao}, processSyncAction: ${nChamada}); ` +
        "a Baileys desta imagem não é a conferida, o ajuste não foi aplicado",
    );
  }
  return fonte
    .replace(ANCORA_FUNCAO, `${FUNCAO}${ANCORA_FUNCAO}`)
    .replace(
      ANCORA_CHAMADA,
      `${ANCORA_CHAMADA}\n                klozzerRepassarCallLog(mutation, ev, authState.creds.me, isInitialSync, signalRepository, logger);`,
    );
}

async function principal(pastaBaileys) {
  const pacote = JSON.parse(readFileSync(join(pastaBaileys, "package.json"), "utf8"));
  if (pacote.version !== VERSAO_CONFERIDA) {
    throw new Error(`Baileys ${pacote.version} nesta imagem; o ajuste foi conferido só na ${VERSAO_CONFERIDA}`);
  }
  const arquivo = join(pastaBaileys, "lib", "Socket", "chats.js");
  const antes = readFileSync(arquivo, "utf8");
  const depois = aplicarPatchNoChats(antes);
  if (depois === antes) {
    console.log("klozzer call-log: ajuste já estava aplicado");
  } else {
    writeFileSync(arquivo, depois);
    console.log("klozzer call-log: ajuste aplicado em", arquivo);
  }
  // Prova de que o módulo ajustado carrega (sintaxe e imports) antes de a
  // imagem existir. Erro aqui derruba o build, nunca o servidor.
  const modulo = await import(pathToFileURL(arquivo).href);
  if (typeof modulo.makeChatsSocket !== "function") {
    throw new Error("chats.js ajustado não exporta makeChatsSocket");
  }
  const final = readFileSync(arquivo, "utf8");
  if (contar(final, "klozzerRepassarCallLog(mutation") !== 1 || contar(final, MARCA) !== 1) {
    throw new Error("o ajuste não ficou no arquivo exatamente uma vez");
  }
  console.log("klozzer call-log: módulo carregou com o ajuste");
}

const executadoDireto = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (executadoDireto) {
  const pasta = process.argv[2];
  if (!pasta) {
    console.error("uso: node patch-chamadas.mjs <pasta node_modules/baileys>");
    process.exit(2);
  }
  principal(pasta).catch((e) => {
    console.error("klozzer call-log:", e instanceof Error ? e.message : String(e));
    process.exit(1);
  });
}
