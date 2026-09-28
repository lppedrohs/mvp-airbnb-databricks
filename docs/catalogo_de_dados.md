# Catálogo de Dados

Transcrição do catálogo gravado no Unity Catalog pelos notebooks (`COMMENT ON TABLE` e `ALTER COLUMN ... COMMENT`), complementada
com o **domínio observado** de cada coluna, calculado sobre os dados reais (mínimo e máximo para números e datas; lista de valores
para colunas com poucas categorias) e com o **% de nulos**. A coluna *Descrição e linhagem* é o comentário gravado no Unity Catalog:
traz o significado, o domínio esperado e a origem (tabela e coluna de origem ou regra de derivação).

- [Camada Bronze](#camada-bronze)
- [Camada Silver](#camada-silver)
- [Camada Gold](#camada-gold)
- [Observações do catálogo](#observações-do-catálogo)

## Camada Bronze

Oito tabelas, uma por arquivo da fonte e cidade (`<entidade>_sp` e `<entidade>_rj`). São cópias fiéis dos arquivos do Inside Airbnb,
com **todas as colunas como STRING**, mais quatro metadados de ingestão comuns a todas:

| Coluna | Tipo | Descrição |
|---|---|---|
| `_cidade` | string | Sigla da cidade de origem. Domínio: sp, rj. |
| `_data_snapshot` | date | Data do snapshot na fonte (início da coleta). Domínio: 2026-06-14 (SP), 2026-06-24 (RJ). |
| `_arquivo_origem` | string | Caminho do arquivo no Volume que originou a linha (linhagem). |
| `_ingerido_em` | timestamp | Data e hora da carga na Bronze. |

Abaixo, as colunas da fonte de cada entidade (SP e RJ juntos), com o % de valores vazios e o **destino na Silver**: a coluna em que
o campo foi transformado ou o motivo do descarte. O significado original de cada campo está no
[dicionário de dados do Inside Airbnb](https://docs.google.com/spreadsheets/d/1iWCNJcSutYqpULSQHlNyGInUvHg2BoUGoNRIGa6Szc4/edit?usp=sharing).

### `bronze.listings_sp` / `bronze.listings_rj`

Linhas: **91.067** (SP + RJ) · Colunas da fonte: **90**

| Coluna da fonte | % vazio | Destino na Silver |
|---|---|---|
| `id` | 0,0% | anuncios.id_anuncio |
| `listing_url` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `scrape_id` | 0,0% | descartada: fora do escopo das perguntas |
| `last_scraped` | 0,0% | anuncios.data_coleta |
| `source` | 0,0% | descartada: fora do escopo das perguntas |
| `name` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `description` | 2,3% | descartada: texto livre ou dado do perfil, fora do escopo |
| `neighborhood_overview` | 100,0% | descartada: 100% vazia (DQ-16) |
| `picture_url` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_id` | 0,0% | anuncios.id_anfitriao |
| `host_url` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_profile_id` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_profile_url` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_name` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_since` | 100,0% | descartada: 100% vazia (DQ-16) |
| `hosts_time_as_user_years` | 0,0% | descartada: fora do escopo das perguntas |
| `hosts_time_as_user_months` | 0,0% | descartada: fora do escopo das perguntas |
| `hosts_time_as_host_years` | 0,0% | anuncios.meses_como_anfitriao (anos x 12) |
| `hosts_time_as_host_months` | 0,0% | anuncios.meses_como_anfitriao (+ meses) |
| `host_location` | 21,9% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_about` | 48,6% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_response_time` | 100,0% | descartada: 100% vazia (DQ-16) |
| `host_response_rate` | 100,0% | descartada: 100% vazia (DQ-16) |
| `host_acceptance_rate` | 100,0% | descartada: 100% vazia (DQ-16) |
| `host_is_superhost` | 0,0% | anuncios.anfitriao_superhost |
| `host_thumbnail_url` | 100,0% | descartada: 100% vazia (DQ-16) |
| `host_picture_url` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_neighbourhood` | 100,0% | descartada: 100% vazia (DQ-16) |
| `host_listings_count` | 0,0% | descartada: fora do escopo das perguntas |
| `host_total_listings_count` | 100,0% | descartada: 100% vazia (DQ-16) |
| `host_verifications` | 0,0% | descartada: texto livre ou dado do perfil, fora do escopo |
| `host_has_profile_pic` | 0,0% | descartada: fora do escopo das perguntas |
| `host_identity_verified` | 0,0% | anuncios.anfitriao_identidade_verificada |
| `neighbourhood` | 100,0% | descartada: 100% vazia (DQ-16) |
| `neighbourhood_cleansed` | 0,0% | anuncios.bairro |
| `neighbourhood_group_cleansed` | 53,5% | descartada: só em SP; o grupo vem de neighbourhoods (DQ-04) |
| `latitude` | 0,0% | anuncios.latitude |
| `longitude` | 0,0% | anuncios.longitude |
| `property_type` | 0,0% | anuncios.tipo_propriedade |
| `room_type` | 0,0% | anuncios.tipo_quarto |
| `accommodates` | 0,0% | anuncios.capacidade_hospedes |
| `bathrooms` | 10,9% | anuncios.banheiros |
| `bathrooms_text` | 0,1% | descartada: fora do escopo das perguntas |
| `bedrooms` | 14,0% | anuncios.quartos |
| `beds` | 6,4% | anuncios.camas |
| `amenities` | 0,0% | anuncios.qtd_comodidades (contagem) |
| `price` | 5,2% | anuncios.preco_diaria |
| `price_quote_checkin_date` | 4,0% | descartada: fora do escopo das perguntas |
| `price_quote_checkout_date` | 4,0% | descartada: fora do escopo das perguntas |
| `price_quote_total_price` | 5,2% | descartada: fora do escopo das perguntas |
| `price_quote_price_per_night` | 5,2% | descartada: fora do escopo das perguntas |
| `price_quote_raw` | 4,0% | descartada: fora do escopo das perguntas |
| `minimum_nights` | 0,0% | anuncios.noites_minimas |
| `maximum_nights` | 0,0% | anuncios.noites_maximas |
| `minimum_minimum_nights` | 0,0% | descartada: fora do escopo das perguntas |
| `maximum_minimum_nights` | 0,0% | descartada: fora do escopo das perguntas |
| `minimum_maximum_nights` | 0,0% | descartada: fora do escopo das perguntas |
| `maximum_maximum_nights` | 0,0% | descartada: fora do escopo das perguntas |
| `minimum_nights_avg_ntm` | 0,0% | descartada: fora do escopo das perguntas |
| `maximum_nights_avg_ntm` | 0,0% | descartada: fora do escopo das perguntas |
| `calendar_updated` | 100,0% | descartada: 100% vazia (DQ-16) |
| `has_availability` | 0,6% | descartada: fora do escopo das perguntas |
| `availability_30` | 0,0% | descartada: fora do escopo das perguntas |
| `availability_60` | 0,0% | descartada: fora do escopo das perguntas |
| `availability_90` | 0,0% | descartada: fora do escopo das perguntas |
| `availability_365` | 0,0% | anuncios.dias_disponiveis_365 |
| `calendar_last_scraped` | 0,0% | descartada: fora do escopo das perguntas |
| `number_of_reviews` | 0,0% | anuncios.num_avaliacoes |
| `number_of_reviews_ltm` | 0,0% | anuncios.num_avaliacoes_12m |
| `number_of_reviews_l30d` | 0,0% | descartada: fora do escopo das perguntas |
| `availability_eoy` | 0,0% | descartada: fora do escopo das perguntas |
| `number_of_reviews_ly` | 0,0% | descartada: fora do escopo das perguntas |
| `estimated_occupancy_l365d` | 0,0% | anuncios.noites_ocupadas_estimadas_12m |
| `estimated_revenue_l365d` | 5,2% | anuncios.receita_estimada_12m |
| `first_review` | 17,1% | anuncios.data_primeira_avaliacao |
| `last_review` | 17,1% | anuncios.data_ultima_avaliacao |
| `review_scores_rating` | 17,1% | anuncios.nota_geral |
| `review_scores_accuracy` | 17,1% | descartada: fora do escopo das perguntas |
| `review_scores_cleanliness` | 17,1% | anuncios.nota_limpeza |
| `review_scores_checkin` | 17,1% | descartada: fora do escopo das perguntas |
| `review_scores_communication` | 17,1% | descartada: fora do escopo das perguntas |
| `review_scores_location` | 17,1% | anuncios.nota_localizacao |
| `review_scores_value` | 17,1% | anuncios.nota_custo_beneficio |
| `license` | 100,0% | descartada: 100% vazia (DQ-16) |
| `instant_bookable` | 100,0% | descartada: 100% vazia (DQ-16) |
| `calculated_host_listings_count` | 0,0% | anuncios.qtd_anuncios_anfitriao_cidade |
| `calculated_host_listings_count_entire_homes` | 0,0% | descartada: fora do escopo das perguntas |
| `calculated_host_listings_count_private_rooms` | 0,0% | descartada: fora do escopo das perguntas |
| `calculated_host_listings_count_shared_rooms` | 0,0% | descartada: fora do escopo das perguntas |
| `reviews_per_month` | 17,1% | anuncios.avaliacoes_por_mes |

### `bronze.calendar_sp` / `bronze.calendar_rj`

Linhas: **33.254.058** (SP + RJ) · Colunas da fonte: **5**

| Coluna da fonte | % vazio | Destino na Silver |
|---|---|---|
| `listing_id` | 0,0% | calendario.id_anuncio |
| `date` | 0,0% | calendario.data |
| `available` | 0,0% | calendario.disponivel |
| `minimum_nights` | 0,0% | calendario.noites_minimas |
| `maximum_nights` | 0,0% | calendario.noites_maximas |

### `bronze.reviews_sp` / `bronze.reviews_rj`

Linhas: **2.965.987** (SP + RJ) · Colunas da fonte: **6**

| Coluna da fonte | % vazio | Destino na Silver |
|---|---|---|
| `listing_id` | 0,0% | avaliacoes.id_anuncio |
| `id` | 0,0% | avaliacoes.id_avaliacao |
| `date` | 0,0% | avaliacoes.data_avaliacao |
| `reviewer_id` | 0,0% | descartada: dado pessoal ou texto livre (DQ-12) |
| `reviewer_name` | 0,0% | descartada: dado pessoal ou texto livre (DQ-12) |
| `comments` | 0,0% | descartada: dado pessoal ou texto livre (DQ-12) |

### `bronze.neighbourhoods_sp` / `bronze.neighbourhoods_rj`

Linhas: **256** (SP + RJ) · Colunas da fonte: **2**

| Coluna da fonte | % vazio | Destino na Silver |
|---|---|---|
| `neighbourhood_group` | 62,5% | bairros.grupo_bairro |
| `neighbourhood` | 0,0% | bairros.bairro |

## Camada Silver

### `silver.bairros`

Silver | Lista oficial de bairros de São Paulo e Rio de Janeiro usada pelo Inside Airbnb. Grão: 1 linha por cidade + bairro. Origem: bronze.neighbourhoods_sp e bronze.neighbourhoods_rj (união). grupo_bairro preenchido apenas em SP, com a subprefeitura; NULL no RJ (DQ-04).

Linhas: **256**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze. | rj, sp | 0,0% |
| `cidade` | string | Nome da cidade. Domínio: São Paulo, Rio de Janeiro. Derivado de sigla_cidade. | Rio de Janeiro, São Paulo | 0,0% |
| `bairro` | string | Nome oficial do bairro (espaços extras removidos). Origem: neighbourhoods.neighbourhood. | 254 valores distintos (ex.: Campo Grande, Penha, Abolição) | 0,0% |
| `grupo_bairro` | string | Agrupamento de bairros: subprefeitura em SP (32 valores); NULL no RJ, onde a fonte não publica. Origem: neighbourhoods.neighbourhood_group. | 32 valores distintos (ex.: Se, Lapa, Mooca) | 62,5% |
| `data_snapshot` | date | Data de coleta do snapshot na fonte. Origem: metadado _data_snapshot da Bronze. | 2026-06-14 a 2026-06-24 | 0,0% |

### `silver.anuncios`

Silver | Anúncios do Airbnb em São Paulo e Rio de Janeiro, limpos, tipados e centralizados. Grão: 1 linha por anúncio (id_anuncio). Origem: bronze.listings_sp e bronze.listings_rj (união por nome). Registros inválidos removidos (sem id, fora do município, capacidade <= 0, duplicados). Flags preco_valido, outlier_preco, estadia_longa e anuncio_ativo indicam restrições para análise (tratamentos DQ-01 a DQ-15). Valores monetários em reais (BRL).

Linhas: **91.067**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_anuncio` | bigint | Identificador do anúncio (chave primária). BIGINT convertido de texto para evitar perda de precisão (DQ-02). Origem: listings.id | 91.067 valores distintos | 0,0% |
| `id_anfitriao` | bigint | Identificador do anfitrião. Origem: listings.host_id | 43.336 valores distintos | 0,0% |
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze. | rj, sp | 0,0% |
| `cidade` | string | Nome da cidade. Domínio: São Paulo, Rio de Janeiro. | Rio de Janeiro, São Paulo | 0,0% |
| `bairro` | string | Bairro oficial, obtido pela fonte a partir da coordenada; chave para silver.bairros (DQ-04). Origem: listings.neighbourhood_cleansed | 248 valores distintos (ex.: Copacabana, Itaim Bibi, Ipanema) | 0,0% |
| `latitude` | double | Latitude (graus decimais, anonimizada pela fonte em ~150 m). Validada contra o limite do município. Origem: listings.latitude | -23,9532 a -22,7505 | 0,0% |
| `longitude` | double | Longitude (graus decimais). Validada contra o limite do município. Origem: listings.longitude | -46,8128 a -43,1036 | 0,0% |
| `tipo_propriedade` | string | Tipo de propriedade declarado (ex.: Entire rental unit, Private room in home). Origem: listings.property_type | 93 valores distintos (ex.: Entire rental unit, Private room in rental unit, Entire home) | 0,0% |
| `tipo_quarto` | string | Tipo de acomodação. Domínio: Entire home/apt, Private room, Shared room, Hotel room. Origem: listings.room_type | Entire home/apt, Hotel room, Private room, Shared room | 0,0% |
| `capacidade_hospedes` | int | Número máximo de hóspedes (> 0). Origem: listings.accommodates | 1 a 16 | 0,0% |
| `quartos` | int | Número de quartos (pode ser NULL). Origem: listings.bedrooms | 0 a 46 | 14,0% |
| `camas` | int | Número de camas (pode ser NULL). Origem: listings.beds | 1 a 64 | 6,4% |
| `banheiros` | double | Número de banheiros (pode ser NULL). Origem: listings.bathrooms | 0,5 a 20 | 10,9% |
| `qtd_comodidades` | int | Quantidade de comodidades listadas, contada a partir do array JSON. Origem: listings.amenities | 0 a 115 | 0,0% |
| `preco_diaria` | decimal(12,2) | Preço da diária em BRL, convertido de texto como $1,250.00 (DQ-01). NULL quando ausente na fonte. Origem: listings.price | 5,54 a 574.013 | 5,2% |
| `noites_minimas` | int | Estadia mínima exigida, em noites. Origem: listings.minimum_nights | 1 a 730 | 0,0% |
| `noites_maximas` | int | Estadia máxima permitida, em noites. Origem: listings.maximum_nights | 1 a 1.125 | 0,0% |
| `dias_disponiveis_365` | int | Dias disponíveis para reserva nos próximos 365 dias (0 a 365). Origem: listings.availability_365 | 0 a 365 | 0,0% |
| `noites_ocupadas_estimadas_12m` | int | Noites ocupadas estimadas pela fonte nos últimos 12 meses (modelo baseado em avaliações; 0 a 255 nesta versão). NULL se a versão da fonte não trouxer a coluna. Origem: listings.estimated_occupancy_l365d | 0 a 255 | 0,0% |
| `receita_estimada_12m` | double | Receita estimada pela fonte nos últimos 12 meses em BRL (diária × noites estimadas). Origem: listings.estimated_revenue_l365d | 0 a 6.178.857 | 5,2% |
| `num_avaliacoes` | int | Total de avaliações do anúncio. Origem: listings.number_of_reviews | 0 a 3.314 | 0,0% |
| `num_avaliacoes_12m` | int | Avaliações recebidas nos 12 meses anteriores ao snapshot. Origem: listings.number_of_reviews_ltm | 0 a 1.106 | 0,0% |
| `avaliacoes_por_mes` | double | Média de avaliações por mês desde a primeira avaliação. Origem: listings.reviews_per_month | 0,01 a 73,21 | 17,1% |
| `data_primeira_avaliacao` | date | Data da primeira avaliação (NULL se nunca avaliado). Origem: listings.first_review | 2010-06-07 a 2026-06-25 | 17,1% |
| `data_ultima_avaliacao` | date | Data da avaliação mais recente (NULL se nunca avaliado). Origem: listings.last_review | 2012-02-21 a 2026-06-30 | 17,1% |
| `nota_geral` | double | Nota geral média (0 a 5). NULL quando o anúncio nunca foi avaliado, sem imputação (DQ-08). Origem: listings.review_scores_rating | 0 a 5 | 17,1% |
| `nota_limpeza` | double | Nota média de limpeza (0 a 5). Origem: listings.review_scores_cleanliness | 1 a 5 | 17,1% |
| `nota_localizacao` | double | Nota média de localização (0 a 5). Origem: listings.review_scores_location | 1 a 5 | 17,1% |
| `nota_custo_beneficio` | double | Nota média de custo-benefício (0 a 5). Origem: listings.review_scores_value | 1 a 5 | 17,1% |
| `meses_como_anfitriao` | int | Tempo como anfitrião, em meses (anos x 12 + meses; 0 a ~190). Substitui host_since, vazio na fonte (DQ-16). Origem: listings.hosts_time_as_host_years e hosts_time_as_host_months | 0 a 185 | 0,0% |
| `anfitriao_superhost` | boolean | Anfitrião com selo Superhost (t/f convertido para boolean). Origem: listings.host_is_superhost | true / false | 0,0% |
| `anfitriao_identidade_verificada` | boolean | Identidade do anfitrião verificada (boolean). Origem: listings.host_identity_verified | true / false | 0,0% |
| `qtd_anuncios_anfitriao_cidade` | int | Anúncios do anfitrião no snapshot da cidade, calculado pela fonte (DQ-09). Origem: listings.calculated_host_listings_count | 1 a 496 | 0,0% |
| `preco_valido` | boolean | Flag (DQ-05): true quando preco_diaria está preenchido e é maior que zero. Derivada. | true / false | 0,0% |
| `outlier_preco` | boolean | Flag (DQ-06): preço fora de exp(Q1 - 1,5*IQR) a exp(Q3 + 1,5*IQR) do log do preço, por cidade e tipo_quarto. Derivada. | true / false | 0,0% |
| `estadia_longa` | boolean | Flag (DQ-07): noites_minimas >= 30, perfil de aluguel mensal e não de temporada. Derivada. | true / false | 0,0% |
| `anuncio_ativo` | boolean | Flag (DQ-07): recebeu ao menos 1 avaliação nos 12 meses anteriores ao snapshot (num_avaliacoes_12m > 0). Derivada. | true / false | 0,0% |
| `data_coleta` | date | Data em que a fonte coletou o anúncio. Origem: listings.last_scraped | 2026-06-14 a 2026-07-01 | 0,0% |
| `data_snapshot` | date | Data do snapshot da cidade na fonte. Origem: metadado _data_snapshot da Bronze. | 2026-06-14 a 2026-06-24 | 0,0% |

### `silver.calendario`

Silver | Calendário de disponibilidade dos anúncios de SP e RJ para os 365 dias seguintes ao snapshot. Grão: 1 linha por anúncio e dia (id_anuncio, data). Origem: bronze.calendar_sp e bronze.calendar_rj (união). Somente anúncios presentes em silver.anuncios. A fonte não publica preço diário no calendário (DQ-10); a diária está em silver.anuncios.

Linhas: **33.239.458**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_anuncio` | bigint | Identificador do anúncio; chave estrangeira para silver.anuncios. Origem: calendar.listing_id | 91.067 valores distintos | 0,0% |
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze. | rj, sp | 0,0% |
| `data` | date | Dia do calendário. Domínio: 365 dias a partir da coleta (SP: 2026-06-14 a 2027-06-15; RJ: 2026-06-25 a 2027-06-30). Origem: calendar.date | 2026-06-14 a 2027-06-30 | 0,0% |
| `disponivel` | boolean | true = dia livre para reserva; false = reservado OU bloqueado pelo anfitrião (a fonte não diferencia, DQ-11). Origem: calendar.available (t/f) | true / false | 0,0% |
| `noites_minimas` | int | Estadia mínima exigida para check-in neste dia. Origem: calendar.minimum_nights | 1 a 1.125 | 0,0% |
| `noites_maximas` | int | Estadia máxima permitida para check-in neste dia. Origem: calendar.maximum_nights | 1 a 1.125 | 0,0% |
| `data_snapshot` | date | Data do snapshot da cidade na fonte. Origem: metadado _data_snapshot da Bronze. | 2026-06-14 a 2026-06-24 | 0,0% |

### `silver.avaliacoes`

Silver | Avaliações de hóspedes dos anúncios de SP e RJ. Grão: 1 linha por avaliação (id_avaliacao). Origem: bronze.reviews_sp e bronze.reviews_rj (união). Nome do avaliador, id do avaliador e comentário removidos por minimização de dados pessoais (DQ-12). Somente anúncios presentes em silver.anuncios.

Linhas: **2.964.344**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_avaliacao` | bigint | Identificador da avaliação (chave primária). BIGINT convertido de texto (DQ-02). Origem: reviews.id | 2.964.344 valores distintos | 0,0% |
| `id_anuncio` | bigint | Anúncio avaliado; chave estrangeira para silver.anuncios. Origem: reviews.listing_id | 75.473 valores distintos | 0,0% |
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. Origem: metadado _cidade da Bronze. | rj, sp | 0,0% |
| `data_avaliacao` | date | Data da avaliação. Domínio: 2008-01-01 até a última data de coleta da cidade (DQ-17). Origem: reviews.date | 2010-06-07 a 2026-06-30 | 0,0% |
| `data_snapshot` | date | Data do snapshot da cidade na fonte. Origem: metadado _data_snapshot da Bronze. | 2026-06-14 a 2026-06-24 | 0,0% |

## Camada Gold

### `gold.dim_bairro`

Gold | Dimensão de localização: bairros oficiais de São Paulo e Rio de Janeiro. Grão: 1 linha por cidade + bairro. Origem: silver.bairros.

Linhas: **256**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `sk_bairro` | bigint | Chave substituta do bairro (hash xxhash64 de sigla_cidade + bairro). Derivada. | 256 valores distintos | 0,0% |
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. | rj, sp | 0,0% |
| `cidade` | string | Nome da cidade. Domínio: São Paulo, Rio de Janeiro. | Rio de Janeiro, São Paulo | 0,0% |
| `bairro` | string | Nome oficial do bairro (96 em SP, 160 no RJ). Origem: silver.bairros.bairro. | 254 valores distintos (ex.: Campo Grande, Penha, Abolição) | 0,0% |
| `grupo_bairro` | string | Subprefeitura em SP (32 valores); NULL no RJ. Origem: silver.bairros.grupo_bairro. | 32 valores distintos (ex.: Se, Lapa, Mooca) | 62,5% |

### `gold.dim_anfitriao`

Gold | Dimensão de anfitrião, com portfólio recalculado sobre a base unificada SP + RJ (DQ-09). Grão: 1 linha por anfitrião. Origem: silver.anuncios agregado por id_anfitriao.

Linhas: **43.336**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_anfitriao` | bigint | Identificador do anfitrião (chave). Origem: silver.anuncios.id_anfitriao. | 43.336 valores distintos | 0,0% |
| `qtd_anuncios_total` | bigint | Anúncios do anfitrião nas duas cidades (>= 1). Derivada: contagem em silver.anuncios. | 1 a 616 | 0,0% |
| `qtd_anuncios_sp` | bigint | Anúncios do anfitrião em São Paulo (>= 0). Derivada. | 0 a 496 | 0,0% |
| `qtd_anuncios_rj` | bigint | Anúncios do anfitrião no Rio de Janeiro (>= 0). Derivada. | 0 a 169 | 0,0% |
| `qtd_cidades` | bigint | Em quantas cidades o anfitrião atua. Domínio: 1 ou 2. Derivada. | 1 a 2 | 0,0% |
| `anfitriao_superhost` | boolean | Anfitrião com selo Superhost (boolean). Origem: silver.anuncios.anfitriao_superhost. | true / false | 0,0% |
| `anfitriao_identidade_verificada` | boolean | Identidade verificada pela plataforma (boolean). Origem: silver.anuncios. | true / false | 0,0% |
| `meses_como_anfitriao` | int | Tempo como anfitrião em meses (maior valor entre os anúncios). Origem: silver.anuncios. | 0 a 185 | 0,0% |
| `perfil_anfitriao` | string | Perfil pelo tamanho do portfólio. Domínio: 1 - Ocasional (1 anúncio), 2 - Pequeno (2 a 4), 3 - Profissional (5 ou mais). Derivada. | 1 - Ocasional, 2 - Pequeno, 3 - Profissional | 0,0% |

### `gold.dim_imovel`

Gold | Dimensão do imóvel anunciado: tipo e características físicas. Grão: 1 linha por anúncio. Origem: silver.anuncios.

Linhas: **91.067**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_anuncio` | bigint | Identificador do anúncio (chave). Origem: silver.anuncios.id_anuncio. | 91.067 valores distintos | 0,0% |
| `tipo_propriedade` | string | Tipo de propriedade declarado (ex.: Entire rental unit). Origem: silver.anuncios. | 93 valores distintos (ex.: Entire rental unit, Private room in rental unit, Entire home) | 0,0% |
| `tipo_quarto` | string | Tipo de acomodação. Domínio: Entire home/apt, Private room, Shared room, Hotel room. Origem: silver.anuncios. | Entire home/apt, Hotel room, Private room, Shared room | 0,0% |
| `capacidade_hospedes` | int | Número máximo de hóspedes (1 a 16). Origem: silver.anuncios. | 1 a 16 | 0,0% |
| `faixa_capacidade` | string | Faixa de capacidade. Domínio: 1) 1 a 2, 2) 3 a 4, 3) 5 a 6, 4) 7 ou mais. Derivada. | 1) 1 a 2, 2) 3 a 4, 3) 5 a 6, 4) 7 ou mais | 0,0% |
| `quartos` | int | Número de quartos (pode ser NULL). Origem: silver.anuncios. | 0 a 46 | 14,0% |
| `camas` | int | Número de camas (pode ser NULL). Origem: silver.anuncios. | 1 a 64 | 6,4% |
| `banheiros` | double | Número de banheiros (pode ser NULL). Origem: silver.anuncios. | 0,5 a 20 | 10,9% |
| `qtd_comodidades` | int | Quantidade de comodidades listadas. Origem: silver.anuncios. | 0 a 115 | 0,0% |

### `gold.dim_data`

Gold | Dimensão de calendário. Grão: 1 linha por dia, da avaliação mais antiga ao fim do calendário de disponibilidade. Gerada.

Linhas: **6.233**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `data` | date | Dia (chave). | 2010-06-07 a 2027-06-30 | 0,0% |
| `ano` | int | Ano. | 2.010 a 2.027 | 0,0% |
| `mes` | int | Mês (1 a 12). | 1 a 12 | 0,0% |
| `ano_mes` | string | Ano e mês no formato yyyy-MM. | 205 valores distintos (ex.: 2010-07, 2010-08, 2010-10) | 0,0% |
| `trimestre` | int | Trimestre (1 a 4). | 1 a 4 | 0,0% |
| `dia_semana` | int | Dia da semana (1 = domingo ... 7 = sábado). | 1 a 7 | 0,0% |
| `nome_dia_semana` | string | Nome do dia da semana em português. | domingo, quarta, quinta, segunda, sexta, sábado, terça | 0,0% |
| `fim_de_semana` | boolean | true para sábado e domingo. | true / false | 0,0% |

### `gold.fato_anuncio`

Gold | Fato central: 1 linha por anúncio com preço, ocupação e receita anual estimadas, disponibilidade futura, demanda e notas. Origem: silver.anuncios + dim_bairro + resumos de fato_disponibilidade_mensal e fato_avaliacao. Rentabilidade = receita bruta anual estimada (BRL); a base não traz o valor do imóvel.

Linhas: **91.067**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_anuncio` | bigint | Identificador do anúncio (chave); chave estrangeira para dim_imovel. | 91.067 valores distintos | 0,0% |
| `id_anfitriao` | bigint | Anfitrião; chave estrangeira para dim_anfitriao. | 43.336 valores distintos | 0,0% |
| `sk_bairro` | bigint | Bairro; chave estrangeira para dim_bairro. | 250 valores distintos | 0,0% |
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. | rj, sp | 0,0% |
| `preco_diaria` | decimal(12,2) | Preço da diária em BRL (NULL quando a fonte não informa, DQ-05). Origem: silver.anuncios. | 5,54 a 574.013 | 5,2% |
| `noites_ocupadas_estimadas_12m` | int | Noites ocupadas nos últimos 12 meses estimadas pela fonte (0 a 255). Origem: silver.anuncios. | 0 a 255 | 0,0% |
| `taxa_ocupacao_estimada` | double | noites_ocupadas_estimadas_12m / 365 (0 a ~0,70). Derivada. | 0 a 0,6986 | 0,0% |
| `receita_estimada_12m` | decimal(14,2) | Receita bruta anual estimada em BRL = preco_diaria x noites_ocupadas_estimadas_12m (igual à estimativa da fonte). Derivada. | 0 a 6.178.856,55 | 5,2% |
| `dias_disponiveis_365` | int | Dias livres nos próximos 365 dias segundo a fonte (0 a 365). Origem: silver.anuncios. | 0 a 365 | 0,0% |
| `pct_dias_indisponiveis_futuro` | double | Parcela dos próximos 12 meses reservada ou bloqueada (0 a 1; DQ-11). Derivada de fato_disponibilidade_mensal. | 0 a 1 | 0,0% |
| `num_avaliacoes` | int | Total de avaliações do anúncio. Origem: silver.anuncios. | 0 a 3.314 | 0,0% |
| `avaliacoes_365d` | bigint | Avaliações nos 365 dias anteriores à coleta do anúncio. Derivada de fato_avaliacao. | 0 a 1.106 | 0,0% |
| `nota_geral` | double | Nota geral (0 a 5; NULL se nunca avaliado, DQ-08). Origem: silver.anuncios. | 0 a 5 | 17,1% |
| `nota_limpeza` | double | Nota de limpeza (0 a 5). Origem: silver.anuncios. | 1 a 5 | 17,1% |
| `nota_localizacao` | double | Nota de localização (0 a 5). Origem: silver.anuncios. | 1 a 5 | 17,1% |
| `nota_custo_beneficio` | double | Nota de custo-benefício (0 a 5). Origem: silver.anuncios. | 1 a 5 | 17,1% |
| `preco_valido` | boolean | Flag DQ-05 (preço preenchido e > 0). Origem: silver.anuncios. | true / false | 0,0% |
| `outlier_preco` | boolean | Flag DQ-06 (fora dos limites de Tukey no log do preço). Origem: silver.anuncios. | true / false | 0,0% |
| `estadia_longa` | boolean | Flag DQ-07 (estadia mínima >= 30 noites). Origem: silver.anuncios. | true / false | 0,0% |
| `anuncio_ativo` | boolean | Flag DQ-07 (ao menos 1 avaliação em 12 meses). Origem: silver.anuncios. | true / false | 0,0% |
| `elegivel_analise` | boolean | true quando preco_valido e anuncio_ativo, sem outlier_preco e sem estadia_longa: base das análises de preço e rentabilidade. Derivada. | true / false | 0,0% |
| `data_snapshot` | date | Data do snapshot da cidade na fonte. | 2026-06-14 a 2026-06-24 | 0,0% |

### `gold.fato_disponibilidade_mensal`

Gold | Disponibilidade futura dos anúncios por mês (próximos 12 meses a partir da coleta). Grão: 1 linha por anúncio e mês. Origem: silver.calendario agregado por mês. Indisponível = reservado ou bloqueado pelo anfitrião (DQ-11).

Linhas: **1.179.958**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_anuncio` | bigint | Anúncio; chave estrangeira para dim_imovel e fato_anuncio. | 91.067 valores distintos | 0,0% |
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. | rj, sp | 0,0% |
| `mes_referencia` | date | Primeiro dia do mês; chave estrangeira para dim_data. Domínio: 2026-06-01 a 2027-06-01. | 2026-06-01 a 2027-06-01 | 0,0% |
| `dias_no_calendario` | bigint | Dias do mês presentes no calendário (1 a 31; menor no primeiro e no último mês). Derivada: contagem. | 5 a 31 | 0,0% |
| `dias_disponiveis` | bigint | Dias livres para reserva no mês. Derivada: soma de silver.calendario.disponivel. | 0 a 31 | 0,0% |
| `dias_indisponiveis` | bigint | Dias reservados ou bloqueados no mês. Derivada. | 0 a 31 | 0,0% |
| `pct_dias_indisponiveis` | double | dias_indisponiveis / dias_no_calendario (0 a 1). Derivada. | 0 a 1 | 0,0% |

### `gold.fato_avaliacao`

Gold | Avaliações de hóspedes (evento de demanda). Grão: 1 linha por avaliação. Origem: silver.avaliacoes + anfitrião e bairro de silver.anuncios e dim_bairro (JOIN por id_anuncio).

Linhas: **2.964.344**

| Coluna | Tipo | Descrição e linhagem | Domínio observado | % nulos |
|---|---|---|---|---|
| `id_avaliacao` | bigint | Identificador da avaliação (chave). Origem: silver.avaliacoes. | 2.964.344 valores distintos | 0,0% |
| `id_anuncio` | bigint | Anúncio avaliado; chave estrangeira para dim_imovel e fato_anuncio. | 75.473 valores distintos | 0,0% |
| `id_anfitriao` | bigint | Anfitrião do anúncio; chave estrangeira para dim_anfitriao. Origem: JOIN com silver.anuncios. | 35.377 valores distintos | 0,0% |
| `sk_bairro` | bigint | Bairro do anúncio; chave estrangeira para dim_bairro. Origem: JOIN com dim_bairro. | 248 valores distintos | 0,0% |
| `sigla_cidade` | string | Sigla da cidade. Domínio: sp, rj. | rj, sp | 0,0% |
| `data_avaliacao` | date | Data da avaliação; chave estrangeira para dim_data. Domínio: 2010-06-07 até o fim da coleta. | 2010-06-07 a 2026-06-30 | 0,0% |

## Observações do catálogo

- **Nomes de bairro repetidos entre cidades:** "Campo Grande" e "Penha" existem em SP e no RJ (256 bairros, 254 nomes). Por isso
  a chave de bairro é sempre cidade + bairro (`sk_bairro` na Gold) e nenhuma análise agrupa só pelo nome.
- **Grafia dos bairros de SP sem acento:** a fonte publica os 96 distritos de São Paulo sem acentos (`Se`, `Agua Rasa`, `Bras`,
  `Sao Miguel`), enquanto os 160 bairros do RJ têm acentuação (`Gávea`, `Méier`). A grafia da fonte foi mantida em todas as camadas
  para preservar a correspondência com os arquivos originais; os textos das análises usam a grafia oficial.
- **`data_snapshot` é o início da coleta:** a coleta dura alguns dias (SP: 14 a 16/06; RJ: 25/06 a 01/07), por isso `data_coleta` e
  `data_avaliacao` passam da data do snapshot (DQ-17).
- **Nota geral igual a 0:** 2 anúncios, com uma única avaliação e as demais notas iguais a 5, têm `nota_geral = 0`, um valor
  inconsistente. Nenhum dos dois é elegível para as análises, então não há impacto nos resultados.
- **Valores extremos preservados:** `preco_diaria` (até R$ 574.013), `receita_estimada_12m` (até R$ 6,2 milhões), `camas` (até 64) e
  `noites_minimas` (até 730 nos anúncios e 1.125 no calendário) são mantidos na Silver e tratados por flags (DQ-06 e DQ-07), não
  por remoção.
- **`noites_ocupadas_estimadas_12m` tem teto de 255 noites**, definido pelo modelo de estimativa da fonte; por isso a taxa de ocupação
  estimada vai no máximo a 0,70.
- **IDs** são exibidos como quantidade de valores distintos: chaves primárias têm tantos valores distintos quanto linhas.
