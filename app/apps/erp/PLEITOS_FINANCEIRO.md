# Pleitos do Financeiro — tipos, subtipos, campos e condições

> **Gerado pelo sistema a partir de `core/titulos/pleitos.py`.** Não edite
> este arquivo à mão: mude o catálogo e rode
> `python -m app.apps.erp.core.titulos.pleitos` — há teste que recusa o
> arquivo diferente do código. É o mesmo catálogo que o portal desenha, que
> a tela Lançar do ERP lê e que o servidor confere nas duas portas.

## 1. Formas de pagamento — o que cada uma pede

| Forma | Conta do credor | Linha digitável |
|---|---|---|
| Boleto | não aparece | obrigatória |
| Pix | obrigatória | não aparece |
| Transferência (TED) | obrigatória | não aparece |
| Guia (DARF, GPS, DAM, FGTS) | não aparece | opcional |
| Débito automático | não aparece | não aparece |

Campo que não aparece para a forma escolhida é **descartado** pelo servidor — nas duas portas.

## 2. Pedir pagamento

Campos que valem para todos os subtipos, na ordem da tela (os do subtipo entram depois de "Conta do credor"):

| Campo | Aparece | Obrigatório |
|---|---|---|
| O que está sendo pago | sempre | sim |
| Credor (quem recebe) | sempre | sim |
| Forma de pagamento | sempre | sim |
| Conta do credor | só se Pix ou Transferência (TED) | só se Pix ou Transferência (TED) |
| O que foi comprado ou contratado | sempre | sim (mín. 5 letras) |
| Valor do documento | sempre | sim |
| Obra | sempre | sim |
| Conta do plano (categoria) | sempre | sim |
| Vencimento e valor (uma linha por parcela) | sempre | sim |
| ↳ Linha digitável (em cada parcela) | só se Boleto ou Guia (DARF, GPS, DAM, FGTS) | só se Boleto |

### Os subtipos

| Subtipo | Onde | Formas que servem | Nº do documento | Data | Campos próprios | Documentos |
|---|---|---|---|---|---|---|
| **Nota de material (NF-e)** | portal e ERP | Boleto, Pix, Transferência (TED) | Nº da nota fiscal — obrigatório | Data de emissão — obrigatória | Chave de acesso (44 dígitos) (opcional) | Nota fiscal (obrigatório); Boleto (só se Boleto) |
| **Nota de serviço (NFS-e)** | portal e ERP | Boleto, Pix, Transferência (TED) | Nº da NFS-e — obrigatório | Data de emissão — obrigatória | — | Nota fiscal (obrigatório); Boleto (só se Boleto) |
| **Frete (CT-e)** | portal e ERP | Boleto, Pix, Transferência (TED) | Nº do CT-e — obrigatório | Data de emissão — obrigatória | — | O CT-e (conhecimento de transporte) (obrigatório); Boleto (só se Boleto) |
| **Serviço de pessoa física (RPA)** | portal e ERP | Pix, Transferência (TED) | Nº do RPA — opcional | Data do recibo — obrigatória | — | Recibo / RPA assinado (obrigatório) |
| **Conta de consumo (água, luz, telefone, internet)** | portal e ERP | Boleto, Débito automático, Pix | Nº da fatura ou da unidade consumidora — opcional | não aparece | Mês de referência (obrigatório) | Fatura da concessionária (obrigatório) |
| **Guia de imposto ou taxa (DARF, GPS, DAM, FGTS)** | portal e ERP | Guia (DARF, GPS, DAM, FGTS) | Nº da guia — opcional | não aparece | Competência (obrigatório) | Guia (obrigatório) |
| **Despesa sem nota fiscal (exceção)** | portal e ERP | Pix, Transferência (TED), Boleto | Nº do recibo — opcional | Data do recibo — opcional | Por que não há nota (obrigatório) | Recibo ou comprovante (obrigatório) |
| **Folha de pagamento ou encargos** | ERP | Pix, Transferência (TED), Guia (DARF, GPS, DAM, FGTS), Boleto | Nº do documento — opcional | não aparece | Competência da folha (obrigatório) | nenhum exigido |
| **Parcela de financiamento ou empréstimo** | ERP | Boleto, Débito automático, Transferência (TED) | Nº do contrato ou da parcela — opcional | não aparece | — | Boleto (só se Boleto) |
| **Adiantamento a fornecedor** | ERP | Pix, Transferência (TED), Boleto | Nº da proposta ou do pedido — opcional | não aparece | — | Boleto (só se Boleto) |

## 3. Fundo fixo (prestação de contas)

| Que prestação | O que é | Período |
|---|---|---|
| **Reembolso — gastei do meu bolso** | despesas pagas com dinheiro seu, que a empresa devolve | opcional |
| **Prestação de contas de adiantamento** | o que foi gasto do dinheiro que a empresa adiantou | obrigatório |

O favorecido é sempre quem presta contas, na chave Pix do cadastro dela. Não há credor a escolher.

| Campo | Aparece | Obrigatório |
|---|---|---|
| Que prestação é esta | sempre | sim |
| Período — de | sempre | só se Prestação de contas de adiantamento |
| Período — até | sempre | só se Prestação de contas de adiantamento |
| As despesas (uma linha por comprovante) | sempre | sim |
| ↳ Data da despesa | sempre | sim |
| ↳ O que foi | sempre | sim (mín. 4 letras) |
| ↳ Onde (estabelecimento) | sempre | não |
| ↳ Valor | sempre | sim |
| ↳ Obra | sempre | sim |
| ↳ Comprovante | sempre | não |

## 4. Cadastro de contrato de empreita

| Como é pago | O que é |
|---|---|
| **Por medição de serviço** | paga o que foi executado: quantidade × preço de cada serviço |
| **Preço fechado, em parcelas** | serviço fechado por um valor, pago em parcelas combinadas |

| Campo | Aparece | Obrigatório |
|---|---|---|
| Como o contrato é pago | sempre | sim |
| Obra | sempre | sim |
| Empreiteiro (empresa ou pessoa) | sempre | sim |
| Serviço contratado | sempre | sim (mín. 8 letras) |
| Planilha de serviços | só se Por medição de serviço | só se Por medição de serviço |
| ↳ Serviço | sempre | sim |
| ↳ Unidade (m², m, un…) | sempre | sim |
| ↳ Quantidade contratada | sempre | sim |
| ↳ Preço unitário | sempre | sim |
| Valor total do contrato | só se Preço fechado, em parcelas | só se Preço fechado, em parcelas |
| Em quantas parcelas | só se Preço fechado, em parcelas | só se Preço fechado, em parcelas |
| Início | sempre | sim |
| Término previsto | sempre | não |
| Retenção de garantia (%) | sempre | não |
| Exige foto em cada medição | sempre | sim |
| Observações | sempre | não |

Documentos: Contrato assinado (quando NÃO é gerado pelo sistema) (opcional).

O contrato nasce **aguardando aval** — quem tem alçada pelo valor aprova no ERP, e só então ele pode ser medido.

### O empreiteiro, pelo documento

No alto do bloco, joga-se o **RG/CNH e o comprovante** (pessoa física) ou o **Cartão CNPJ** (empresa): a leitura preenche os campos, a pessoa confere, e o documento fica arquivado no cadastro do empreiteiro. Quem já está no cadastro é procurado pelo nome ou CPF/CNPJ e o bloco abre preenchido.

| Campo | Aparece | Obrigatório |
|---|---|---|
| Pessoa física / Empresa | sempre | sim |
| CPF ou CNPJ, nome | sempre | sim |
| RG, órgão e UF do RG | só pessoa física | não (entra na qualificação do contrato) |
| Endereço (CEP, rua, número, complemento, bairro, cidade, UF) | sempre | não (entra no contrato) |
| WhatsApp, e-mail | sempre | ver abaixo |

### Contrato gerado pelo sistema e assinado à distância

No cadastro, escolhe-se: **gerar o contrato pelo sistema e colher as assinaturas**, ou **já tenho o contrato assinado** (aí o portal exige o documento anexado).

| Campo | Aparece | Obrigatório |
|---|---|---|
| Quem assina pela empresa (um colaborador) | só se gerar | sim — com telefone ou e-mail no cadastro de colaboradores |
| WhatsApp e e-mail do empreiteiro | sempre, no bloco do empreiteiro | um dos dois, se gerar (é por onde chega o link) |
| Pagar a medição em até (dias) | só se gerar | não (padrão 10) |
| Aviso para rescindir (dias) | só se gerar | não (padrão 15) |
| Condição a mais, do seu jeito (a IA revisa, você aceita ou não) | só se gerar | não |

Cláusulas para marcar (as marcadas por padrão já vêm ligadas):

| Cláusula | Para que serve | Vem marcada | Pede |
|---|---|---|---|
| O empreiteiro fornece o material | por padrão o material é da empresa; marque se for dele | não | — |
| Multa por atraso | por dia de atraso que não for culpa da empresa | não | % ao dia |
| Entregar limpo | retirar entulho e sobras ao terminar cada etapa | sim | — |
| Não pode repassar o serviço | subcontratar só com autorização escrita | sim | — |
| Ajudantes do empreiteiro identificados | quem ele trouxer, ele informa à obra antes de entrar | sim | — |
| Equipamento emprestado pela obra | betoneira, andaime… devolvidos em bom estado | não | Quais equipamentos |
| Nota fiscal a cada medição | para empreiteiro com CNPJ (inclusive MEI) | não | — |
| Autoriza fotos do serviço | as fotos comprovam a medição | sim | — |

**O caminho:** cadastro → aval → os dois recebem o link por WhatsApp e e-mail → cada um abre no celular, pede um código de 6 dígitos (vale 15 minutos), confirma o CPF/CNPJ e assina com o dedo → com as duas assinaturas o PDF final (com o registro de cada assinatura e o lacre do texto) vai para o arquivo do contrato, e ele fica **vigente**. Até lá, não se mede.

## 5. Medição de empreita

| Campo | Aparece | Obrigatório |
|---|---|---|
| Contrato | sempre | sim |
| Período medido — de | sempre | sim |
| Período medido — até | sempre | sim |
| Quanto foi executado de cada serviço | só se contrato com planilha | só se contrato com planilha |
| Valor desta medição | só se contrato antigo, sem planilha ou Preço fechado, em parcelas | só se contrato antigo, sem planilha ou Preço fechado, em parcelas |
| Observação | sempre | não |

Documentos: Foto do serviço executado (só se sim). A foto é cobrada na autorização (quem mediu não autoriza a própria medição).

## 6. Cancelar um pedido

| Campo | Aparece | Obrigatório |
|---|---|---|
| Por que cancelar | sempre | sim (mín. 5 letras) |

Quem lançou cancela o que é seu, enquanto ninguém pagou nem conciliou. Depois disso, só estorno, pelo financeiro.

