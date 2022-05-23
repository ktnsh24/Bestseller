DROP TABLE IF EXISTS public.fact_table;

CREATE TABLE fact_table(
    fact_id SERIAL PRIMARY KEY,
    date_key INT,
    asin VARCHAR(255),
    reviewerid VARCHAR(255),
    price INT NULL,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL
);

INSERT INTO public.fact_table (date_key, asin, reviewerid, price)
SELECT DISTINCT
        rdi.date_key,
        pi.asin,
        ri.reviewerid,
        m.price
FROM public.metadata m
LEFT JOIN public.dim_products_info pi ON pi.asin = m.asin
LEFT JOIN public.reviews r ON r.asin = m.asin
LEFT JOIN public.dim_reviewer_info ri ON ri.reviewerid = r.reviewerid
LEFT JOIN public.dim_review_date_info rdi ON rdi.unixReviewTime = r.unixReviewTime;