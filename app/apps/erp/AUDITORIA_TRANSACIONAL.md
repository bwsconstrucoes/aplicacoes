# Consistência transacional — onde uma falha deixa o banco pela metade

Auditoria de 2026-09-02, **sem correção**: só o mapa dos riscos, para decidir
o que vale consertar. Olhou as operações que tocam várias tabelas de uma vez.

## O que protege hoje — e vale para tudo abaixo

`get_session()` (`db/database.py`) faz **rollback em qualquer exceção** e
fecha a sessão no fim. As rotas auditadas abrem **uma** sessão, chamam o core
e dão **um** `commit()` no final. Nenhuma função do core faz commit no meio
(conferido: os únicos `commit()` fora das rotas são os do aplicador de
migrações). O log de auditoria (`registrar_evento`) escreve na **mesma**
sessão. Anexos ficam **no banco** (`anexos.conteudo`), não em disco nem em
Drive — então salvar arquivo e gravar registro são a mesma transação.

Resultado: **nas quatro operações pedidas, uma exceção no meio não deixa
metade gravada.** Ou grava tudo, ou nada. Esse não é o risco.

O risco real é de outra natureza: **duas pessoas fazendo a mesma coisa ao
mesmo tempo**. O gunicorn roda com 4 threads, e o ERP não usa trava de linha
(`FOR UPDATE`) em lugar nenhum. Toda regra do tipo "leio o saldo, confiro,
gravo" pode ser atravessada por outra requisição entre o "leio" e o "gravo".

---

## 1. DC gerando título (`core/pessoal.gerar_titulo`)

Toca: `despesas_colaborador`, `titulos`, `parcelas`, `rateios`,
`titulo_colaboradores`, `eventos`.

| Ponto | O que acontece | Risco |
|---|---|---|
| Falha em qualquer passo | Rollback completo. A DC volta a APROVADA sem título. | **Nenhum.** |
| Dois cliques em "gerar título" ao mesmo tempo | As duas requisições leem `d.titulo_id is None`, as duas passam. `numero_sp` vem de sequence (`nextval`), então **não há erro de unicidade que segure a segunda**: nascem **dois títulos** para a mesma DC, e o segundo `titulo_id` sobrescreve o primeiro. Um título fica órfão e a pagar. | **Médio.** Exige coincidência de segundos, mas o dano é dinheiro pago duas vezes. |
| `d.meio_pagamento = meio` é gravado antes da checagem de `titulo_id` | Coberto pelo rollback. | Nenhum. |

## 2. Medição de empreita consumindo saldo (`core/titulos/empreita`)

Toca (registrar): `contrato_medicoes`, `medicao_itens`, `eventos`.
Toca (autorizar): `contrato_medicoes`, `titulos`, `parcelas`, `rateios`,
`contratos_servico.status`, `eventos`.

| Ponto | O que acontece | Risco |
|---|---|---|
| Falha em qualquer passo | Rollback completo. | **Nenhum.** |
| Duas medições registradas ao mesmo tempo no mesmo contrato | `_saldo_por_item` e `saldo()` são "leio e confiro". As duas passam pela crítica com o saldo antigo. O número da medição é `max+1` — o `UNIQUE (contrato_id, numero)` **derruba a segunda** com erro de banco (500 genérico para o usuário, mas o banco fica íntegro). | **Baixo**, e por acidente: quem segura é a constraint de numeração, não a regra de saldo. Se um dia a numeração mudar para sequence, a proteção some e o saldo do item passa a poder ser **ultrapassado**. |
| Duas autorizações da mesma medição | As duas leem `status == "MEDIDA"`. Não há constraint que impeça dois títulos apontando para a mesma medição. | **Médio.** Dois títulos a pagar para uma medição só. Mesma janela de segundos do caso 1. |

## 3. Devolução de locação ajustando parcelas (`core/locacoes.devolver`)

Toca: `locacao_itens.quantidade_devolvida`, `locacao_movimentos`,
`locacao_parcelas.valor_previsto/status`, `contratos_locacao.status`, `eventos`.

| Ponto | O que acontece | Risco |
|---|---|---|
| Falha em qualquer passo | Rollback completo. | **Nenhum.** |
| Duas devoluções do mesmo item ao mesmo tempo | "Leio disponível, confiro, somo". As duas passam com o disponível antigo: `quantidade_devolvida` pode **ultrapassar a quantidade em obra**, e o valor das parcelas futuras fica errado (ou o contrato encerra indevidamente). | **Médio-baixo.** Devolução é operação de canteiro, raramente simultânea — mas não há nada que impeça. |
| `s.get(ContratoLocacao)` devolve `None` | `c.status = ...` levanta `AttributeError` → 500 → rollback. Não corrompe, mas responde errado. | Cosmético. |

## 4. Conciliação automática (`core/pagamentos/conciliacao.conciliar_automatico`)

Toca: `conciliacoes`, `eventos`.

| Ponto | O que acontece | Risco |
|---|---|---|
| Falha no meio do laço | Rollback: nenhuma conciliação da rodada fica. | **Nenhum.** |
| Duas pessoas executam ao mesmo tempo | `UNIQUE (pagamento_id)` derruba a rodada que chegar depois **inteira** (rollback), não só o par repetido. Quem executou segundo vê erro genérico e roda de novo. | **Baixo.** Íntegro, só incômodo. |
| Mesmo extrato casado com dois pagamentos | Dentro de uma rodada, `extratos_usados` impede. Entre rodadas simultâneas, não há `UNIQUE` em `extrato_id`: dois pagamentos de mesmo valor podem ser "comprovados" pela mesma linha do extrato. | **Baixo**, mas é exatamente o erro que a conciliação existe para pegar. |

---

## O que apareceu no caminho (fora das quatro pedidas)

**Baixa em lote com aviso (`/erp/api/pagamentos/baixar`).** A ordem é:
grava pagamentos → `commit` → manda Telegram para cada um → `commit` dos
registros de envio. Se o segundo commit falhar, o Telegram **já foi** e o
sistema não sabe: na próxima tentativa manda de novo. O oposto (baixa gravada,
aviso falhou) é seguro — há botão de reenvio. Mesmo desenho na baixa por
comprovante (`/erp/api/pagamentos/comprovante`).

**Leitura por IA antes da sessão.** Fatura, contrato e prestação leem o
documento (gastam tokens) **antes** de abrir a sessão. Se o banco falhar
depois, a leitura foi paga e nada foi salvo. Não é inconsistência — é
desperdício, e o painel de IA agora mostra.

**Registro de consumo de IA em sessão própria.** Sobrevive ao rollback da
operação principal. **Intencional**: perder uma linha do painel é aceitável,
perder um lançamento não.

**Migrações.** Cada `.sql` roda na própria transação com o `INSERT` em
`_migracoes`. Se a terceira falhar, as duas primeiras ficam aplicadas — e é o
comportamento certo.

---

## O que NÃO foi olhado

Fechamento da prestação do fundo fixo (`prestacao.criar_prestacao` e
`confirmar_analise`), geração de previsão e lançamento de parcela de locação,
estorno de pagamento, importação de OFX, cancelamento em lote. Todos tocam
mais de uma tabela e merecem a mesma leitura.

---

## Se for corrigir — em ordem de valor

1. **Trava de linha nas três operações "leio, confiro, gravo"**: DC (gerar
   título), medição (registrar e autorizar) e devolução. É um
   `with_for_update()` no `s.get` do registro-pai. Uma linha por função.
2. **`UNIQUE` em `contrato_medicoes.titulo_id` e em `despesas_colaborador.titulo_id`**
   (parciais, onde não nulo): fecham "dois títulos para uma medição/DC" mesmo
   sem a trava, e custam uma migração.
3. **`UNIQUE` parcial em `conciliacoes.extrato_id` onde `desfeita_em IS NULL`.**
4. Na baixa em lote, gravar a intenção de aviso **antes** de enviar (estado
   "ENVIANDO") e confirmar depois — assim reenvio duplicado vira decisão, não
   acidente.

Nenhum dos quatro é urgente hoje: exigem duas pessoas na mesma tela no mesmo
segundo. Mas o sistema está crescendo em usuários, e é o tipo de defeito que
não deixa rastro — só um pagamento a mais no fim do mês.
