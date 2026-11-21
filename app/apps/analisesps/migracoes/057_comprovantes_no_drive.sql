-- ===========================================================================
-- 057 — A PÁGINA DO COMPROVANTE QUE NÃO BAIXOU FICA NO DRIVE (18/11/2026)
--
-- O dono: "Os comprovantes que não foram baixados, eu acho que eles deveriam
-- ficar salvos no Google Drive, até para depois eu posterior analisar que
-- comprovante foi aquele, para eu entender por que ele não baixou, se há alguma
-- regra que não existe ainda."
--
-- O PDF do envio fica no disco do servidor por até sete dias — e some antes, a
-- cada publicação. A página que não baixou (não localizada, recusada, com erro,
-- à espera de validação) passa a ser copiada para o Drive no fim do
-- processamento, e o link fica aqui, por número de página: {"3": "https://…"}.
-- Por LOTE, e não por item, porque "Processar de novo" apaga e regrava os itens
-- e a cópia do Drive não pode se perder (nem ser enviada duas vezes).
-- ===========================================================================
ALTER TABLE analisesps.comprovantes_lote
    ADD COLUMN IF NOT EXISTS paginas_no_drive TEXT NOT NULL DEFAULT '{}';
