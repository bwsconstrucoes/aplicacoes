// -*- coding: utf-8 -*-
//
// IMPRIME O PDF DO MANUAL DO PORTAL, a partir da própria página.
//
// ⚑ Por que existe (25/11/2026): o PDF é uma CÓPIA da página. Mudar o
// `index.html` e não reimprimir deixa os dois diferentes — e ninguém percebe,
// porque cada um abre um dos dois. O manual do ponto aprendeu isso antes
// (`app/apps/ponto/HISTORICO.md`, 15/10/2026).
//
// COMO RODAR, com o ERP de pé em alguma porta:
//
//   SAIDA=app/apps/erp/static/manual_portal/manual-portal-da-obra.pdf \
//   ENDERECO=http://127.0.0.1:5055/erp/portal/manual/ \
//   node app/apps/erp/scripts/imprimir_manual_portal.js
//
// ⚠️ As imagens NÃO podem ter `loading="lazy"`: no PDF elas sairiam pretas.
// Aconteceu com o manual do ponto.
const { chromium } = require('playwright');

(async () => {
  const endereco = process.env.ENDERECO || 'http://127.0.0.1:5055/erp/portal/manual/';
  const saida = process.env.SAIDA
    || 'app/apps/erp/static/manual_portal/manual-portal-da-obra.pdf';
  const navegador = await chromium.launch();
  const pagina = await (await navegador.newContext()).newPage();
  const erros = [];
  pagina.on('pageerror', e => erros.push(e.message));

  await pagina.goto(endereco, { waitUntil: 'networkidle' });
  // O formato de PAPEL é outro: é o `@media print` que põe cada parte numa
  // página e as telas lado a lado.
  await pagina.emulateMedia({ media: 'print' });
  // As dúvidas são <details> recolhidos na tela; no papel, abertos.
  await pagina.evaluate(() => document.querySelectorAll('details').forEach(d => (d.open = true)));
  await pagina.evaluate(async () => {
    await Promise.all([...document.images].filter(i => !i.complete)
      .map(i => new Promise(r => { i.onload = i.onerror = r; })));
    await document.fonts.ready;
  });
  await pagina.waitForTimeout(1500);
  await pagina.pdf({ path: saida, format: 'A4', printBackground: true });

  console.log('PDF gravado em', saida);
  console.log('erros na página:', erros.length ? erros : 'nenhum');
  await navegador.close();
})();
