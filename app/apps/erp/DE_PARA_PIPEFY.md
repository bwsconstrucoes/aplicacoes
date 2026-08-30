# DE-PARA — Pipe "SOLICITAÇÕES FINANCEIRO" → ERP BWS

> Fonte: estrutura exportada via GraphQL (128 campos, 11 fases).
> Filosofia combinada: manter o bom, tirar o ruim, completar o que falta.

## 1. Os 6 procedimentos do pipe e seus destinos no ERP

| Procedimento (pipe) | Destino no ERP |
|---|---|
| **Solicitar Pagamento** | Lançamento de título — dirigido pela **categoria** (tipo interno derivado) |
| **Transferência de Recursos** (conta destino + até 5 origens) | Movimentação de **FLUXO** entre contas da empresa (sem fornecedor); casa com a conciliação dos dois extratos |
| **Fundo Fixo** | Título T10 interno: recarga + prestação de contas com comprovantes |
| **Cancelar SP** | Ação "Cancelar" no título (com motivo, trilha de auditoria) |
| **Outras Solicitações** | Fora do financeiro core — vira tarefa/observação (decidir destino depois) |
| **Aporte** (BWS / Parceria) | **Entrada** de fluxo (aportes) — módulo de entradas/recebimentos (fase 2) |

## 2. O que o ERP preserva do jeito de trabalhar

- Requerente / Responsável pela Solicitação → **solicitante** (login) + campo "requisitado por terceiro" vira observação estruturada; Anuente → aprovador adicional (alçadas).
- Nº da Solicitação → **numero_sp** sequencial automático (SPnnnnnn).
- Parcelas com vencimento/valor/código de barras → iguais, porém **dinâmicas** (sem limite de 10) e com validação estrutural do boleto no ato.
- Rateio entre centros de custo → igual, porém **dinâmico** (sem limite de 5) e amarrado: soma tem que fechar com o líquido.
- Tipo de Pagamento (Transferência/Boleto/Pix/BeeVale) → forma_pagamento (TED/BOLETO/PIX; BeeVale = integração futura, campo preservado no de-para).
- Data para Resposta → prazo/SLA da solicitação (campo previsto).
- Contexto de categoria: abastecimento (veículo, km, litros) e alimentação (períodos, quantidades) → **campos dinâmicos por categoria** (metadados do título), como no pipe.
- Pedido de Compra / Contrato de Locação (connectors) → vínculos pedido_id / contrato_id (three-way match).
- Fases do pipe → status do título: Caixa de entrada→EM_ANALISE · Autorização→AGUARDANDO_APROVACAO · Pré-Análise→análise automática · Realizar Pagamento→APROVADO · Pago/Pago Parcial→PAGO/PAGO_PARCIAL · Cancelado→CANCELADO · Inconsistência→DEVOLVIDO · Falha API→(morre: sem Omie).

## 3. O que morre — e por quê

- **Dados bancários digitados no formulário** (chave Pix, banco/agência/conta) e principalmente **"titular da conta diferente do credor"**: é a porta do golpe da troca de conta (C2). No ERP, pagamento PIX/TED só para **conta homologada no cadastro** do fornecedor. Exceção de titularidade = homologação com dupla checagem, nunca digitação no lançamento.
- **"A despesa gerou NF?" + nº digitado**: substituído por vínculo real de documento fiscal — e pelo lançamento **iniciado pelo documento** (upload XML NFe pré-preenche tudo; PDF na sequência).
- **Limites artificiais** (10 parcelas, 5 rateios): estruturas dinâmicas.
- **Campos de cola** ("Rateio Múltiplo" em texto, links de planilha de análise, campo IA): a análise é nativa do sistema.
- **Fase "Pago/Alimentar Omie" e "Falha API"**: sem Omie, sem retrabalho de alimentação.

## 4. Pipe CENTROS DE CUSTO (218 campos)

Estrutura entendida: cadastro do centro com até 12 objetos/contratos (código p/
emissão de NF e diário, contrato, objeto, valor, endereço, município-UF, OS,
datas, ART, responsáveis, tributação, alíquota ISS, CNO) + Omie + conta de
pagamento + fases de acompanhamento (Criação→Aguardando O.S→Em Execução→
Concluída/Concluída com Dívida/Acervo Técnico/Distratada...).
No ERP: o cadastro de **obras** já recebeu os campos principais (migração 003);
o modelo de "1 centro com N objetos" será tratado como **N obras irmãs**
(sufixos), como a própria numeração do pipe faz. Fases/aditivos/anexos entram
na fase de gestão de obras — fora do financeiro core, integração prevista.
