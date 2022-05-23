DROP TABLE IF EXISTS public.dim_review_date_info;

CREATE TABLE dim_review_date_info(
    date_key SERIAL PRIMARY KEY,
    review_year INT NULL,
    review_month INT NULL,
    review_day INT NULL,
    unixReviewTime TIMESTAMP NULL
);

INSERT INTO public.dim_review_date_info (review_year, review_month, review_day, unixReviewTime)
SELECT DISTINCT ON (unixReviewTime)
    EXTRACT(YEAR FROM unixReviewTime),
    EXTRACT(MONTH FROM unixReviewTime),
    EXTRACT(DAY FROM unixReviewTime),
    unixReviewTime
FROM public.reviews;
