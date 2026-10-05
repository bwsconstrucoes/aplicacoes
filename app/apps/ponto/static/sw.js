// Service worker do "Meu ponto". Guarda SÓ a casca da tela (o HTML do app),
// para ele abrir rápido e aparecer como aplicativo. NUNCA guarda dado: batida,
// espelho e pedidos vêm sempre do servidor, na hora — um espelho velho na tela
// seria pior que nenhum.
const CACHE = "bws-ponto-v2";
// O leitor de QR (jsQR, 250 KB) também: o tablet abre a câmera sem baixar de novo.
const CASCA = ["/ponto/app", "/ponto/app/jsQR.js"];

self.addEventListener("install", e => {
  e.waitUntil(caches.open(CACHE).then(c => c.addAll(CASCA)).then(() => self.skipWaiting()));
});
self.addEventListener("activate", e => {
  e.waitUntil(caches.keys().then(ks => Promise.all(ks.filter(k => k !== CACHE).map(k => caches.delete(k))))
    .then(() => self.clients.claim()));
});
self.addEventListener("fetch", e => {
  const url = new URL(e.request.url);
  if (e.request.method !== "GET" || !CASCA.includes(url.pathname)) return;
  // Rede primeiro; sem rede, a casca guardada (que avisa que está sem internet).
  e.respondWith(fetch(e.request).then(r => {
    const copia = r.clone();
    caches.open(CACHE).then(c => c.put(e.request, copia));
    return r;
  }).catch(() => caches.match(e.request)));
});
