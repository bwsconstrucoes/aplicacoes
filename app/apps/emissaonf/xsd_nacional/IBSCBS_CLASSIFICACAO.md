# IBS/CBS: a classificação tributária e o CST (Anexo VIII)

Isto não é documentação de cortesia: é a regra que custou uma emissão em
08/10/2026 (erro **E0959 — "cClassTrib não pertence ao grupo CST indicado"**).
Fica escrita aqui porque **o schema não a verifica** — os dois campos são
válidos sozinhos, e quem confere a combinação é a plataforma nacional, horas
depois, numa tela de pendências.

## A regra, em uma linha

> **O CST são os três primeiros dígitos do `cClassTrib`.**

Extraído dos dados do `AnexoVIII-CorrelacaoItemNBSIndOpCClassTrib_IBSCBS`, que
veio no pacote oficial da prefeitura. Todos os códigos de classificação da
tabela seguem isso:

| CST | cClassTrib | Nome |
|---|---|---|
| 000 | 000001 | Situações tributadas integralmente pelo IBS e CBS |
| 000 | 000002 | Exploração de via |
| 010 | 010002 | Operações do serviço financeiro |
| 011 | 011001…011005 | Planos de assistência funerária / saúde |
| 200 | 200016…200052 | as reduções (educação, saúde, imóveis, turismo…) |
| 200 | **200046** | **Operações com bens imóveis ← o da BWS** |
| 400 | 400001 | Transporte público coletivo de passageiros |

No código isso é derivado, não digitado: `GrupoIBSCBS.cst` nasce vazio e sai do
`c_class_trib`, e `montar_dps_xml` **recusa** um par que não casa.

## O que a BWS manda, e de onde cada valor saiu

Serviço: **item 07.02** da LC 116 (empreitada de construção civil).

| Campo | Valor | Fonte |
|---|---|---|
| `cTribNac` | `070202` | lista de serviços nacional |
| `cNBS` | `101011100` (1.0101.11.00) | Anexo VIII, linha do item 07.02 |
| `cIndOp` | `020201` | Anexo VIII (aparece como `20201` no Excel, que come o zero da frente; o schema exige **6 dígitos**) |
| `cClassTrib` | `200046` | Anexo VIII — "Operações com bens imóveis" |
| `CST` | `200` | derivado do `cClassTrib` |

## Os dois campos opcionais que ficaram prontos e desligados

Se vier recusa nova **com o CST já casado**, são estes os suspeitos. Estão
implementados em `GrupoIBSCBS` e não são enviados por padrão — mandar valor
fiscal por palpite é pior que omitir um campo opcional.

- **`tpOper`** — "Tipo de Operação com Entes Governamentais ou outros serviços
  sobre bens imóveis": `1` fornecimento com pagamento posterior · `2`
  recebimento do pagamento com fornecimento já realizado · `3` fornecimento com
  pagamento já realizado · `4` recebimento com fornecimento posterior · `5`
  fornecimento e recebimento concomitantes. Para medição faturada e paga depois,
  o candidato é **1**.
- **`tpEnteGov`** — tipo de ente governamental: `1` União · `2` Estado · `3`
  Distrito Federal · `4` Município. **Não dá para deduzir do CNPJ do tomador** —
  tem de ser informado por quem sabe de que esfera é o órgão.

O grupo `imovel` do layout **não** é o nosso caso: o próprio schema diz
"operações relacionadas a bens imóveis, **exceto obras**".
