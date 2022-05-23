DROP TABLE IF EXISTS public.dim_products_info;

CREATE TABLE dim_products_info(
    asin VARCHAR(255) NULL PRIMARY KEY,
    title VARCHAR(10000) NULL,
    price INT NULL,
    imUrl VARCHAR(255) NULL,
    description VARCHAR(50000) NULL
);

INSERT INTO public.dim_products_info (asin, title, price, imUrl, description)
SELECT DISTINCT ON (asin)
    asin,
    title,
    price,
    imUrl,
    description
FROM public.metadata;