# Como me entregar as fórmulas de uma planilha (sem mandar os dados das pessoas)

Escrito em 26/09/2026, porque o dono disse — e está certo:

> *"Você precisa ler as fórmulas das planilhas. Nelas foi criado todo o
> regramento, do contrário você vai criar algo errado. (…) Mas como fazer para
> você ler as fórmulas? Isso é muito importante."*

O acesso que eu tenho às planilhas do Drive devolve **valores**, não fórmulas. E as
planilhas grandes (a do cadastro tem 11,9 MB) não cabem numa leitura só. Então o
caminho é este: **um script que junta só as FÓRMULAS num documento**, e eu leio o
documento.

⚠️ **Por que este caminho e não "baixar a planilha e me mandar":** baixar em `.xlsx`
funcionaria (o Excel guarda as fórmulas), mas aí o arquivo traz **nome, CPF, salário
e endereço de ~500 pessoas** para dentro da conversa. As fórmulas são o que eu
preciso; os dados das pessoas, não. Este script manda só a regra.

---

## O que fazer, uma vez por planilha

1. Abra a planilha (a **Folha de Pagamento - Fortes** é a que mais importa).
2. **Extensões → Apps Script**.
3. Cole o código abaixo num arquivo novo e salve.
4. Rode a função **`exportarFormulas`**. Na primeira vez o Google pede permissão.
5. Ela cria um **documento no seu Drive** chamado
   `FÓRMULAS - <nome da planilha>`. Me diga que está pronto (ou me passe o link) e
   eu leio.

```javascript
/**
 * Junta num documento SÓ as fórmulas desta planilha — nenhum dado de pessoa.
 *
 * Para cada aba e cada coluna, pega a PRIMEIRA fórmula que encontrar e conta
 * quantas linhas repetem aquele mesmo padrão. É isso que interessa: a regra.
 */
function exportarFormulas() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const partes = ["FÓRMULAS — " + ss.getName(),
                  "gerado em " + new Date().toLocaleString("pt-BR"), ""];

  ss.getSheets().forEach(aba => {
    const nome = aba.getName();
    const linhas = Math.min(aba.getLastRow(), 60);   // as primeiras bastam
    const colunas = aba.getLastColumn();
    partes.push("========== ABA: " + nome +
                "  (" + aba.getLastRow() + " linhas x " + colunas + " colunas)");

    if (!linhas || !colunas) { partes.push("(vazia)", ""); return; }

    const formulas = aba.getRange(1, 1, linhas, colunas).getFormulas();
    const cabecalho = aba.getRange(1, 1, Math.min(4, linhas), colunas).getValues();

    for (let c = 0; c < colunas; c++) {
      // O rótulo da coluna: a primeira das 4 primeiras células que tiver texto.
      let rotulo = "";
      for (let r = 0; r < cabecalho.length; r++) {
        const v = String(cabecalho[r][c] || "").trim();
        if (v && v.length < 60) { rotulo = v; break; }
      }
      // A primeira fórmula da coluna, e quantas linhas repetem o padrão.
      let primeira = "", onde = 0, repetidas = 0, distintas = {};
      for (let r = 0; r < linhas; r++) {
        const f = formulas[r][c];
        if (!f) continue;
        if (!primeira) { primeira = f; onde = r + 1; }
        const chave = f.replace(/\d+/g, "#");   // 'A2' e 'A3' viram o mesmo padrão
        distintas[chave] = (distintas[chave] || 0) + 1;
        repetidas++;
      }
      if (!primeira) continue;
      partes.push(letraDaColuna_(c + 1) + "  [" + rotulo + "]" +
                  "  (fórmula na linha " + onde + "; " + repetidas +
                  " linha(s) com fórmula; " + Object.keys(distintas).length +
                  " padrão(ões) diferente(s))");
      partes.push("    " + primeira);
      // Se a coluna tem mais de um padrão, manda também o segundo — é onde mora
      // a exceção que ninguém lembra.
      const chaves = Object.keys(distintas);
      if (chaves.length > 1) {
        for (let r = 0; r < linhas; r++) {
          const f = formulas[r][c];
          if (f && f.replace(/\d+/g, "#") !== primeira.replace(/\d+/g, "#")) {
            partes.push("    (outro padrão, linha " + (r + 1) + ") " + f);
            break;
          }
        }
      }
    }
    partes.push("");
  });

  const doc = DocumentApp.create("FÓRMULAS - " + ss.getName());
  doc.getBody().setText(partes.join("\n"));
  doc.saveAndClose();

  SpreadsheetApp.getUi().alert(
    "Pronto.\n\nCriei no seu Drive o documento:\n\nFÓRMULAS - " + ss.getName() +
    "\n\nNenhum dado de pessoa foi copiado — só as fórmulas.");
}

function letraDaColuna_(numero) {
  let letras = "";
  while (numero > 0) {
    const resto = (numero - 1) % 26;
    letras = String.fromCharCode(65 + resto) + letras;
    numero = Math.floor((numero - 1) / 26);
  }
  return letras;
}
```

---

## Em quais planilhas rodar, e em que ordem

| Ordem | Planilha | Por que importa |
|---|---|---|
| **1ª** | **Folha de Pagamento - Fortes** | é onde está TODO o regramento: as abas `Quinzena`, `Fim de Mês`, `Mobponto`, `DC`, `C. Diários`, e os blocos de rateio por centro de custo e por conta corrente |
| 2ª | Relatório de Análise - Quinzena CTPS | é o relatório que o sistema vai substituir; mostra o que precisa aparecer |
| 3ª | API Connector - Mobponto Relatório de Presença e Cadastros | a coluna A da aba `Presenças` é fórmula, e é ela que amarra o ponto ao cadastro |
| 4ª | Registro de Colaboradores | as colunas calculadas (CPF Números, CPF - Nome, Fase Atual…) |

Se der para rodar só numa, é a **primeira**.

---

## Se o Apps Script não for uma opção

Alternativa sem script, aba por aba: numa aba nova, numa célula, escrever
`=FORMULATEXT(Quinzena!AH2)` (ou a célula que interessa) e me mandar o resultado.
Funciona, mas é uma célula por vez — por isso o script.

---

## O que já foi recuperado sem isso

- **A coluna AH da aba `Mobponto`** — ele colou a fórmula em 26/09/2026, e ela virou
  `app/apps/analisesps/folha_vinculo.py`. Foi ela que mostrou que o vínculo
  (CTPS ou DIÁRIA) é decidido **por DIA**, não por pessoa — eu ia errar isso.
- **O cabeçalho da aba `C. Diários`**, achado por busca no Drive, que revelou que a
  conta corrente vinha sendo carregada da coluna errada (ver `FOLHA_DE_PAGAMENTO.md`
  §7.1, D5).

Ou seja: cada fórmula recuperada até agora consertou um erro. É por isso que vale o
trabalho.
