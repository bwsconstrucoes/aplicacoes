// Gera o PDF (A4) e a imagem de cada página para o WhatsApp dos guias desta
// pasta, a partir do HTML de cada um. Precisa do Playwright (Node).
//
//     node app/apps/erp/orientacao/gerar_guia.js
//
// · guia_do_portal.html → "Acesso ao Portal da Obra.pdf" + guia_portal_N.png (23/11/2026)
// · guia_do_pin.html    → "Como criar o seu PIN.pdf"     + guia_pin_N.png    (25/11/2026)
const path = require("path");
const { chromium } = require("playwright");

const GUIAS = [
  { html: "guia_do_portal.html", pdf: "Acesso ao Portal da Obra.pdf", png: "guia_portal" },
  { html: "guia_do_pin.html", pdf: "Como criar o seu PIN.pdf", png: "guia_pin" },
];

(async () => {
  const aqui = __dirname;
  const nav = await chromium.launch();
  for (const g of GUIAS) {
    const pg = await nav.newPage({ viewport: { width: 794, height: 1123 }, deviceScaleFactor: 2 });
    await pg.goto("file://" + path.join(aqui, g.html));
    await pg.pdf({ path: path.join(aqui, g.pdf), format: "A4", printBackground: true,
                   margin: { top: "0", bottom: "0", left: "0", right: "0" } });
    const folhas = await pg.$$(".folha");
    for (let i = 0; i < folhas.length; i++)
      await folhas[i].screenshot({ path: path.join(aqui, `${g.png}_${i + 1}.png`) });
    await pg.close();
  }
  await nav.close();
})();
