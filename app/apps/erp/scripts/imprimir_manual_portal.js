// -*- coding: utf-8 -*-
//
// IMPRIME O PDF DO MANUAL DO PORTAL, a partir da própria página.
//
// ⚑ Por que existe (25/11/2026): o PDF é uma CÓPIA da página. Mudar o
// `index.html` e não reimprimir deixa os dois diferentes — e ninguém percebe,
// porque cada um abre um dos dois. O manual do ponto aprendeu isso antes
// (`app/apps/ponto/HISTORICO.md`, 15/10/2026).
//
// ⚑ ELE ENTRA NO SISTEMA ANTES DE IMPRIMIR, e isso não é capricho: desde
// 25/11/2026 o manual exige login (decisão do dono — *"não fica exposto esse
// manual. Só para quem tem acesso."*). Sem entrar, o Chromium imprimiria a
// tela de login em 23 páginas, e ninguém olharia o resultado.
//
// COMO RODAR, com o ERP de pé apontando para um banco de TESTE já semeado
// por `scripts/semear_manual_portal.py`:
//
//   ENDERECO=http://127.0.0.1:5055 \
//   node app/apps/erp/scripts/imprimir_manual_portal.js
//
// O usuário e a senha são os que o semeador grava. Para outro ambiente, passe
// EMAIL e SENHA. ⚠️ Nunca ponha aqui senha de produção: este arquivo é
// versionado, e o manual só precisa de um banco de exemplo.
//
// ⚠️ As imagens NÃO podem ter `loading="lazy"`: no PDF elas sairiam pretas.
// Aconteceu com o manual do ponto.
const { chromium } = require('playwright');

(async () => {
  const base = (process.env.ENDERECO || 'http://127.0.0.1:5055').replace(/\/+$/, '');
  const email = process.env.EMAIL || 'marta@exemplo.com';
  const senha = process.env.SENHA || 'OlhadaNaTela1!';
  const saida = process.env.SAIDA
    || 'app/apps/erp/manual_portal/manual-portal-da-obra.pdf';

  const navegador = await chromium.launch();
  const pagina = await (await navegador.newContext()).newPage();
  const erros = [];
  pagina.on('pageerror', e => erros.push(e.message));

  await pagina.goto(base + '/erp/entrar');
  await pagina.fill('input[name=email]', email);
  await pagina.fill('input[name=senha]', senha);
  await pagina.click('button[type=submit]');
  await pagina.waitForLoadState('networkidle');

  await pagina.goto(base + '/erp/portal/manual/', { waitUntil: 'networkidle' });
  if (!/Manual do Portal/.test(await pagina.title())) {
    console.error('NÃO ABRIU O MANUAL — o login falhou? Está em:', pagina.url());
    await navegador.close();
    process.exit(1);
  }
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

  const quebradas = await pagina.evaluate(
    () => [...document.images].filter(i => !i.naturalWidth).length);
  if (quebradas) {
    console.error('ABORTADO:', quebradas, 'imagem(ns) não carregou — o PDF sairia furado.');
    await navegador.close();
    process.exit(1);
  }
  await pagina.pdf({ path: saida, format: 'A4', printBackground: true });

  console.log('PDF gravado em', saida);
  console.log('erros na página:', erros.length ? erros : 'nenhum');
  await navegador.close();
})();
