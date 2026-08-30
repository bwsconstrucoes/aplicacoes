# ERP BWS — Cronograma por HORAS de esforço

> Regra combinada: medir **horas executadas × horas restantes** por módulo,
> não semanas de calendário. Atualizado a cada entrega.
> Última atualização: 13/08/2026.

## Executado — ~15h

| Módulo | Horas | Situação |
|---|---:|---|
| Especificação (matriz A–F, tipos T1–T14, fases) | 2,0 | ✅ documento entregue |
| Schema PostgreSQL (21 tabelas) + models completos | 2,0 | ✅ aplicado no Render e testado |
| Infra Render (erp-db US$6,30/mês) + repo GitHub | 1,0 | ✅ banco no ar; web service pendente de clique |
| Molde fornecedores (service+API+UI) + auth + correções | 1,5 | ✅ rodou no PC do Marcelo |
| Motor de títulos: lançamento dirigido, validações, alçadas, estorno | 2,0 | ✅ testado ponta a ponta |
| Análise automática v1 (score, C7d/E2/A6/E8/C2/D19) | 0,8 | ✅ testado |
| Boleto (linha digitável 47/48, DVs, rolagem 2025) + OFX parser | 1,2 | ✅ testado |
| Pagamentos + conciliação automática/manual | 1,0 | ✅ testado |
| Telas v1 + reescrita no padrão spsbd (nav sticky, filtros na sidebar, KPIs, grade com seleção, ações em lote) | 3,5 | ✅ reescritas |
| Importador CSV obras/categorias + consulta CNPJ no cadastro (BrasilAPI/ReceitaWS) | 1,2 | ✅ criados e testados |

| Lançamento categoria-first (tipo interno derivado) + de-para dos pipes | 1,0 | ✅ testado |
| Lançamento pelo documento: parser XML NFe + vínculo/travas | 1,0 | ✅ testado |
| Proposta do plano financeiro novo (78 categorias, resultado×fluxo) | 0,5 | ✅ aguardando validação |

## Restante até o financeiro operacional — ~22–34h

| Entrega | Horas | Observação |
|---|---:|---|
| Web service Render no ar + DATABASE_URL + 1º deploy validado | 1–2 | travado só no clique final |
| Plano financeiro novo (estrutura resultado×fluxo) + importação | 3–5 | definição junto com Marcelo* |
| DECISÃO: sem cargas do Omie — sistema começa limpo (fornecedores via consulta CNPJ) | 0 | ✅ decidido |
| Ajustes de uso nas telas (feedback do financeiro) | 4–6 | inclui campos que faltarem |
| Leitura de PDF (recibos/NFSe) no lançamento por documento | 3–5 | após validar o fluxo XML |
| Robô Bradesco registrando baixa via core | 1–2 | adaptar BradescoRafaelV2 |

\* metade dessas horas é decisão sua (duplicidades, de-para), não código.

## Fases seguintes (estimativa macro)
Captura DFe/SEFAZ + NFSe Eusébio: 12–18h · Motor fiscal completo
(retenções automáticas, guias T8 vinculadas): 10–14h · Dossiê Lucro Real: 6–10h.
