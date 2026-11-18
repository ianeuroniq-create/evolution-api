/**
 * Ajuste da Baileys dentro da imagem da Evolution: confirmar (ack) a mensagem
 * de STATUS que o WhatsApp manda solta, como `<status>`.
 *
 * Por que existe (18/11/2026, pedido do MATHEUS, "acabar com a zumbi de uma
 * vez"): desde o fim de setembro de 2026 o WhatsApp passou a mandar parte das
 * atualizações de status dos contatos como uma estrofe `<status>` própria, e
 * não mais como `<message from="status@broadcast">`. A Baileys 7.0.0-rc.9 não
 * tem quem escute `CB:status`, então essa estrofe nunca recebe o ack. Uns 50
 * minutos depois o servidor do WhatsApp derruba a conexão com
 * `stream:error` > `<ack class="status" .../>` ("stream errored out" no log).
 * A Evolution reconecta em segundos, o servidor reentrega o MESMO status, que
 * de novo fica sem ack, e a fila de entrega do número trava atrás dele: a
 * instância fica "open" e não recebe mais nada. Reiniciar a instância ou o
 * container não cura, porque o status pendente continua no servidor; só um
 * pareamento novo (QR) cura, e por pouco tempo.
 *
 * Conferido no log do servidor em 18/11/2026: várias linhas por minuto de
 * `"reasonNode":{"tag":"ack","attrs":{"class":"status","type":"media",...}}`,
 * e o ciclo de queda de 3.000 segundos por número que o Klozzer media.
 * Referências: WhiskeySockets/Baileys#2858 (PR #2866, aberto, sem versão) e
 * evolution-foundation/evolution-api#2778.
 *
 * O que o ajuste faz, e só isso: registra `ws.on('CB:status', ...)` logo
 * antes de `ws.on('CB:ack,class:message', ...)` em `lib/Socket/messages-recv.js`,
 * e o handler chama o `sendMessageAck` da própria Baileys. Esse ack sai com
 * `class="status"` e repete `type`, `participant` e `recipient` da estrofe, SEM
 * código de erro (um ack com erro, o NACK, deixa a estrofe pendente no servidor
 * e o problema continua). A Evolution já ignora status (`readStatus: false`),
 * então nada que o Klozzer usa deixa de chegar.
 *
 * Regras que importam num servidor que atende TODAS as organizações:
 *  - o handler nunca lança: a promessa do ack tem `.catch` e o erro vai para o
 *    `onUnexpectedError` da Baileys (log), sem derrubar a conexão;
 *  - o handler não passa pelo `processNode`: no rc.9 o processador da fila
 *    offline não conhece o tipo `status` e travaria a fila ("unknown offline
 *    node type"), que é justamente o defeito que se quer tirar;
 *  - o build falha se a Baileys não for a versão conferida, se a âncora não
 *    aparecer exatamente uma vez, ou se o arquivo resultante não carregar.
 *
 * Quando a Baileys publicar a correção oficial (#2866) e a Evolution subir de
 * versão, este ajuste sai: o build falha de propósito na versão nova.
 *
 * Uso (no Dockerfile): node patch-status-ack.mjs /evolution/node_modules/baileys
 */

import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

export const VERSAO_CONFERIDA = "7.0.0-rc.9";
export const MARCA = "/* klozzer:status-ack v1 */";
export const ANCORA = "    ws.on('CB:ack,class:message', (node) => {";

export const CODIGO_DO_HANDLER = [
  `    ${MARCA}`,
  "    ws.on('CB:status', (node) => {",
  "        sendMessageAck(node).catch(error => onUnexpectedError(error, 'klozzer: ack de status'));",
  "    });",
].join("\n");

const contar = (texto, trecho) => texto.split(trecho).length - 1;

/** Insere o handler antes da âncora. Lança se o arquivo não for o conferido. */
export function aplicarPatchNoMessagesRecv(fonte) {
  if (fonte.includes(MARCA)) {
    throw new Error("patch-status-ack: o ajuste já está aplicado neste arquivo");
  }
  if (contar(fonte, "ws.on('CB:status'") !== 0) {
    throw new Error("patch-status-ack: a Baileys já escuta CB:status; o ajuste não é mais necessário");
  }
  if (contar(fonte, ANCORA) !== 1) {
    throw new Error("patch-status-ack: âncora CB:ack,class:message não aparece exatamente uma vez");
  }
  if (!fonte.includes("const sendMessageAck = async (") || !fonte.includes("onUnexpectedError")) {
    throw new Error("patch-status-ack: sendMessageAck ou onUnexpectedError não estão no arquivo");
  }
  return fonte.replace(ANCORA, `${CODIGO_DO_HANDLER}\n${ANCORA}`);
}

async function principal(pastaBaileys) {
  const pacote = JSON.parse(readFileSync(join(pastaBaileys, "package.json"), "utf8"));
  if (pacote.version !== VERSAO_CONFERIDA) {
    throw new Error(
      `patch-status-ack: Baileys ${pacote.version}, conferida só a ${VERSAO_CONFERIDA}. Confira se a versão nova já confirma <status> (Baileys #2866) antes de mexer aqui.`,
    );
  }
  const arquivo = join(pastaBaileys, "lib", "Socket", "messages-recv.js");
  const saida = aplicarPatchNoMessagesRecv(readFileSync(arquivo, "utf8"));
  writeFileSync(arquivo, saida);
  // Carregar o módulo prova que o arquivo continua válido (sintaxe e imports).
  await import(pathToFileURL(arquivo).href);
  console.log(`patch-status-ack: aplicado em ${arquivo}`);
}

const executadoDireto = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (executadoDireto) {
  const pasta = process.argv[2];
  if (!pasta) {
    console.error("uso: node patch-status-ack.mjs /evolution/node_modules/baileys");
    process.exit(2);
  }
  principal(pasta).catch((erro) => {
    console.error(erro instanceof Error ? erro.message : erro);
    process.exit(1);
  });
}
