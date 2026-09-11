// ===========================================================================
// ERP BWS — service worker
//
// A REGRA QUE MANDA NESTE ARQUIVO: ERP NÃO SERVE NÚMERO DE CACHE.
//
// O caminho normal de um service worker é guardar as respostas e devolvê-las
// rápido quando a rede demora. Num site de conteúdo isso é ótimo. Aqui seria
// perigoso: a pessoa abriria o ERP no celular, veria "R$ 480.000 a pagar" que
// ficou guardado ontem, e tomaria decisão em cima disso — sem nada na tela
// avisando que o número é velho. Número errado com cara de certo é o que este
// sistema inteiro tenta evitar.
//
// Então aqui só entra no cache o que NÃO É DADO: a folha de estilo e os
// ícones. Toda rota do ERP e toda /api/ vai à rede, sempre. Sem internet, a
// navegação cai numa página que DIZ que está sem internet — nunca num número.
//
// O que o service worker entrega, então? O ícone no celular: é ele que torna
// o ERP instalável, abrindo em tela cheia, sem a barra do navegador.
// ===========================================================================
const VERSAO = "erp-v1";
const CASCA = [
  "/erp/static/erp.css",
  "/erp/static/icone-192.png",
  "/erp/static/icone-512.png",
];

self.addEventListener("install", ev => {
  ev.waitUntil(caches.open(VERSAO).then(c => c.addAll(CASCA))
    .then(() => self.skipWaiting()));
});

self.addEventListener("activate", ev => {
  ev.waitUntil(caches.keys()
    .then(nomes => Promise.all(nomes.filter(n => n !== VERSAO).map(n => caches.delete(n))))
    .then(() => self.clients.claim()));
});

// Só GET, e só o que é arquivo estático nosso. Qualquer outra coisa nem passa
// por aqui — vai direto à rede, como se o service worker não existisse.
function eArquivoEstatico(url) {
  return url.origin === self.location.origin
      && url.pathname.startsWith("/erp/static/");
}

self.addEventListener("fetch", ev => {
  const req = ev.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);

  if (eArquivoEstatico(url)) {
    // Rede primeiro mesmo aqui: assim uma publicação nova chega na hora, e o
    // cache só socorre quando a rede falhou.
    ev.respondWith(
      fetch(req).then(resp => {
        const copia = resp.clone();
        caches.open(VERSAO).then(c => c.put(req, copia));
        return resp;
      }).catch(() => caches.match(req))
    );
    return;
  }

  // Navegação (abrir uma tela) sem internet: página honesta, não dado velho.
  if (req.mode === "navigate") {
    ev.respondWith(fetch(req).catch(() => new Response(
      `<!doctype html><meta charset="utf-8">
       <meta name="viewport" content="width=device-width, initial-scale=1">
       <title>Sem internet — ERP BWS</title>
       <style>
         body{margin:0;min-height:100vh;display:flex;align-items:center;
              justify-content:center;background:#0A1B2E;color:#fff;
              font:16px/1.6 system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
              padding:24px;text-align:center}
         .caixa{max-width:420px}
         h1{font-size:20px;margin:0 0 10px}
         p{color:#B9C3D6;margin:0 0 18px}
         b{color:#FFB92E}
         button{background:#E8A317;border:0;color:#0A1B2E;font-weight:700;
                padding:12px 20px;border-radius:8px;font-size:15px}
       </style>
       <div class="caixa">
         <h1>Sem internet</h1>
         <p>O ERP precisa de conexão para mostrar qualquer número.
            <b>De propósito:</b> guardar valor no celular faria você ver
            o saldo de ontem achando que é o de hoje.</p>
         <button onclick="location.reload()">Tentar de novo</button>
       </div>`,
      {headers: {"Content-Type": "text/html; charset=utf-8"}, status: 503}
    )));
  }
  // Todo o resto (as /api/, os relatórios, os anexos): nem tocamos.
});
