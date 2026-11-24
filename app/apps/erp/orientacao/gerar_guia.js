// Gera o PDF (A4) e a imagem de cada página para o WhatsApp do guia de acesso
// ao portal, a partir de guia_do_portal.html (uma página desde 23/11/2026). Precisa do Playwright (Node).
//
//     node app/apps/erp/orientacao/gerar_guia.js
const path = require("path");
const { chromium } = require("playwright");

(async () => {
  const aqui = __dirname;
  const nav = await chromium.launch();
  const pg = await nav.newPage({ viewport: { width: 794, height: 1123 }, deviceScaleFactor: 2 });
  await pg.goto("file://" + path.join(aqui, "guia_do_portal.html"));
  await pg.pdf({ path: path.join(aqui, "Acesso ao Portal da Obra.pdf"), format: "A4",
                 printBackground: true, margin: { top: "0", bottom: "0", left: "0", right: "0" } });
  const folhas = await pg.$$(".folha");
  for (let i = 0; i < folhas.length; i++)
    await folhas[i].screenshot({ path: path.join(aqui, `guia_portal_${i + 1}.png`) });
  await nav.close();
})();
