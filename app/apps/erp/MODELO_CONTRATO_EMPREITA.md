# Contrato de empreitada para pequenos serviços — RASCUNHO para revisão

> **Rascunho de 27/11/2026, para o dono e o advogado revisarem.** Ainda não está
> no sistema. Os campos entre `{{ }}` são preenchidos pelo ERP a partir do
> cadastro: da empresa do grupo, do empreiteiro, da obra e do contrato.
>
> Serve para o serviço pequeno contratado direto com pessoa física ou
> microempresa: muro, piso, reboco, pintura. O contrato com empresa maior
> continua sendo o dela.
>
> ⚠️ **Este texto não foi visto por advogado.** Falta conferir sobretudo a
> cláusula 9, sobre vínculo de emprego, e a retenção de INSS quando o
> empreiteiro é pessoa física.

---

## CONTRATO DE PRESTAÇÃO DE SERVIÇOS POR EMPREITADA Nº {{contrato.numero}}

**CONTRATANTE:** {{empresa.razao_social}}, inscrita no CNPJ sob o nº
{{empresa.cnpj}}, com sede em {{empresa.endereco_completo}}, neste ato
representada por {{representante.nome}}, {{representante.cargo}}, CPF
{{representante.cpf}}.

**CONTRATADO:** {{empreiteiro.nome}}, {{empreiteiro.documento_tipo}} nº
{{empreiteiro.documento}}, residente/sediado em {{empreiteiro.endereco_completo}},
telefone {{empreiteiro.telefone}}, e-mail {{empreiteiro.email}}.

As partes contratam, por este instrumento, a execução de serviço por
empreitada, nos termos dos artigos 610 a 626 do Código Civil e das cláusulas
abaixo.

### Cláusula 1 — Objeto

1.1. O CONTRATADO executará, com mão de obra própria, o serviço de
**{{contrato.objeto}}** na obra **{{obra.codigo}} — {{obra.nome}}**, situada em
{{obra.endereco}}.

1.2. O serviço compreende os itens abaixo, com as quantidades e os preços
combinados:

{{#se contrato.por_medicao}}
| Serviço | Unidade | Quantidade | Preço unitário | Total |
|---|---|---|---|---|
{{#cada contrato.itens}}| {{descricao}} | {{unidade}} | {{quantidade}} | {{preco_unitario}} | {{total}} |
{{/cada}}
{{/se}}
{{#se contrato.em_parcelas}}
Serviço por preço fechado, conforme descrito no item 1.1.
{{/se}}

### Cláusula 2 — Prazo

2.1. Início em **{{contrato.data_inicio}}** e término previsto para
**{{contrato.data_fim}}**.

2.2. O atraso que não for causado pela CONTRATANTE deve ser comunicado por
escrito, com o motivo, assim que for conhecido.

### Cláusula 3 — Preço

3.1. O valor total estimado deste contrato é de **{{contrato.valor_total}}
({{contrato.valor_por_extenso}})**.

{{#se contrato.por_medicao}}
3.2. O valor devido é apurado por **medição**: a quantidade efetivamente
executada de cada serviço, multiplicada pelo seu preço unitário. Serviço não
executado não é pago. Quantidade além da contratada só é paga com aditivo
escrito.
{{/se}}
{{#se contrato.em_parcelas}}
3.2. O valor é pago em **{{contrato.parcelas_previstas}} parcela(s)**, cada uma
liberada pela conferência da etapa correspondente.
{{/se}}

3.3. O preço inclui mão de obra, ferramentas e equipamentos de uso pessoal do
CONTRATADO, transporte próprio e todos os encargos da sua atividade. Os
materiais são fornecidos pela CONTRATANTE, salvo acordo escrito em contrário.

### Cláusula 4 — Medição e pagamento

4.1. A medição é feita por período, com a quantidade executada de cada serviço,
**fotos do serviço** e observações, registradas no sistema da CONTRATANTE.

4.2. A medição é conferida e autorizada por pessoa da CONTRATANTE com alçada.
Autorizada, o pagamento é feito em até **{{prazo_pagamento_dias}} dias**, por
Pix ou transferência para a conta de titularidade do CONTRATADO
({{empreiteiro.conta}}). Pagamento a terceiro só com autorização escrita.

4.3. Do valor de cada medição pode ser descontado:
a) o adiantamento eventualmente concedido;
b) a retenção de garantia de **{{contrato.retencao_garantia_pct}}%**, devolvida
   após a conclusão e a aceitação do serviço;
c) os tributos que a lei mandar reter.

### Cláusula 5 — Obrigações do CONTRATADO

5.1. Executar o serviço com boa técnica, seguindo as normas aplicáveis, o
projeto e as orientações técnicas da obra quanto ao resultado esperado.

5.2. Usar os equipamentos de proteção individual exigidos e cumprir as normas
de segurança do canteiro (NR-18).

5.3. Responder pelos seus ajudantes, se os tiver, inclusive por pagamento,
encargos e segurança, sem nenhum vínculo deles com a CONTRATANTE.

5.4. Refazer, sem custo, o serviço executado com defeito.

5.5. Manter regular a sua situação perante os órgãos públicos e apresentar,
quando pedido, o comprovante de recolhimento dos seus tributos.

### Cláusula 6 — Obrigações da CONTRATANTE

6.1. Dar acesso ao local, fornecer os materiais combinados e as informações
técnicas necessárias.

6.2. Medir e pagar nos prazos deste contrato.

### Cláusula 7 — Garantia do serviço

7.1. O CONTRATADO responde pela solidez e pela segurança do serviço nos termos
do artigo 618 do Código Civil, e pelos defeitos aparentes que forem apontados
na aceitação.

### Cláusula 8 — Rescisão

8.1. Qualquer das partes pode rescindir este contrato com aviso escrito de
**{{aviso_rescisao_dias}} dias**. Paga-se o serviço medido até a data.

8.2. O abandono do serviço, ou o descumprimento das cláusulas 5.1 a 5.4,
permite a rescisão imediata, descontado o custo de corrigir o que ficou mal
feito.

### Cláusula 9 — Natureza do contrato

9.1. Este é um contrato civil de empreitada. O CONTRATADO executa o serviço com
**autonomia** quanto à forma, ao horário e aos meios, responde pelo resultado e
pode prestar serviço a outros. Não há subordinação, horário fixo nem
exclusividade, e por isso não há vínculo de emprego entre as partes.

> ⚠️ **Para o dono e o advogado.** O papel só protege se a prática for a
> mesma. Se o empreiteiro bate ponto, recebe ordens de horário e trabalha
> todo dia como se fosse da equipe, o contrato não impede a Justiça do Trabalho
> de reconhecer vínculo. O que protege, além do texto, é o que o sistema já
> registra: pagamento por medição do que foi executado, não por dia, e
> contrato com prazo e objeto definidos.

### Cláusula 10 — Assinatura eletrônica e foro

10.1. As partes aceitam a assinatura eletrônica deste contrato, com
identificação por código enviado ao telefone ou e-mail informado e registro
de data, hora e lacre do documento, nos termos da Lei 14.063/2020 e do
artigo 10, § 2º, da MP 2.200-2/2001.

10.2. Fica eleito o foro de {{empresa.municipio}}/{{empresa.uf}}.

{{empresa.municipio}}, {{data_por_extenso}}.

| CONTRATANTE | CONTRATADO |
|---|---|
| {{representante.nome}} — {{empresa.razao_social}} | {{empreiteiro.nome}} |
| {{assinatura.contratante}} | {{assinatura.contratado}} |

---

## O que o cadastro ainda NÃO tem para preencher este texto

| Dado | Situação hoje |
|---|---|
| Quem assina pela empresa (nome, CPF, cargo) | **não existe** no cadastro da empresa |
| Endereço da obra | existe no cadastro da obra |
| Endereço, telefone e e-mail do empreiteiro | existem no credor, mas costumam estar em branco quando ele é cadastrado na hora |
| Prazo de pagamento após a medição, aviso de rescisão | não existem — seriam configuração (padrão sugerido: 10 dias e 15 dias) |
| Valor por extenso | o sistema calcula |
