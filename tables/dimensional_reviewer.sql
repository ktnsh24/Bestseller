DROP TABLE IF EXISTS public.dim_reviewer_info;

CREATE TABLE dim_reviewer_info(
    reviewerid VARCHAR(255) NULL PRIMARY KEY,
    reviewername VARCHAR(255) NULL
);

INSERT INTO public.dim_reviewer_info (reviewerid, reviewername)
SELECT DISTINCT ON (r.reviewerid)
    r.reviewerid,
    r.reviewername
FROM public.reviews r;