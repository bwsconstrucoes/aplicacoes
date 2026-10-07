# Schemas oficiais da NFS-e Nacional (DPS) — layout 1.01

Estes arquivos **não foram escritos aqui**. São os schemas oficiais, do pacote
`Layout_EL_DPS_Nacional` que a prefeitura de Eusébio distribui no portal, em
Configurações › APIs de Integração. O dono baixou e passou o pacote em
07/10/2026, no dia em que a prefeitura desativou o modelo antigo.

**Por que estão versionados aqui, e não só no PC de alguém:** emitir nota é a
única coisa deste repositório que não se desfaz. Com o schema dentro do
repositório, o teste `tests/test_emissaonf_dps.py` confere a declaração contra a
regra oficial **antes** de ela sair — então um campo fora de ordem, um valor fora
do domínio ou uma casa decimal a mais é pego aqui, não na prefeitura.

Sem isso, a única forma de descobrir um erro de estrutura seria emitindo.

**Não editar.** Quando a prefeitura publicar um layout novo, substituir os
arquivos inteiros pelos novos e rodar a suíte — o teste vai dizer o que mudou.

Do pacote original ficaram de fora o manual em PDF, os exemplos em XML e as
planilhas de anexo (correlação NBS e lista de serviços). Eles não são lidos por
código nenhum; o que o código precisa saber está escrito no `README.md` e no
`HISTORICO.md` da área.
