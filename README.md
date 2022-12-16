Escopo do projeto:

Consumir os dados a partir de uma API, fazer o processo de ETL e, por fim, inserir o resultado em um banco de dados, utilizando o orquestrador de pipeline airflow rodando no container docker.

Aproveitando o bom desempenho da produção agrícola no país vou utilizar os dados do Sistema SIDRA - Banco de tabelas estatísticas que tem como objetivo armazenar e disponibilizar os dados de pesquisas realizadas pelo Instituto Brasileiro de Geografia e Estatística (IBGE) – mais especificamente os dados da produção agrícola dos anos de 2021 e 2022 por cultura, área plantada e área colhida. Mais informações sobre a API pode ser obtido em https://servicodados.ibge.gov.br/api/docs/agregados?versao=3

O arquivo depois de tratado deve ter o layout:


coluna        tipo

id_sidra      int (identificador único)

unit          Varchar (Toneladas ou Hectares)

values        Int (quantidade)

types         Varchar (Área Plantada ou Colhida)

year_month    Date (data de coleta dos dados)

year_harvest  Int (ano da Safra)

product       Varchar (cultura)

created_at    Timestamp (data de criação do arquivo)
