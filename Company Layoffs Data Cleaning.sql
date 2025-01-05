#Data cleaning 

select *
from layoffs;

create table layoffs_new
like layoffs;

select *
from layoffs_new;

insert layoffs_new
select *
from layoffs;

#Duplicates

select *
from layoffs_new;

select *,
row_number() over(partition by company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions)as row_num
from layoffs_new;

with duplicate_cte as 
(
select *,
row_number() over(partition by company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions)as row_num
from layoffs_new
)
select * 
from duplicate_cte
where row_num > 1 ;

select*
from layoffs_new
where company = 'casper'

CREATE TABLE `layoffs_new2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int 
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_new2;

insert into layoffs_new2
select *,
row_number() over(partition by company,location,industry,total_laid_off,`date`,stage,country,funds_raised_millions)as row_num
from layoffs_new;

select * 
from layoffs_new2
where row_num > 1;


Delete  
from layoffs_new2
where row_num > 1;


#standardizing data 

select company,trim(company)
from layoffs_new2;

update layoffs_new2
set company = trim(company);

select distinct industry
from layoffs_new2 
order by 1;

select *
from layoffs_new2
where industry like 'crypto%';

update layoffs_new2
set industry ='crypto'
where industry like 'crypto%'
;

select distinct country
from layoffs_new2 
order by 1;

select distinct country,trim(trailing '.' from country)
from layoffs_new2 
order by 1;

update layoffs_new2
set country = trim(trailing '.' from country)
where country like 'United state%';

select `date`,
str_to_date(`date` , '%m/%d/%Y')
from layoffs_new2 ;

update layoffs_new2
set `date` = str_to_date(`date` , '%m/%d/%Y')
;

alter table layoffs_new2
modify column `date` date;

select *
from layoffs_new2
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_new2
where industry is null 
or industry = '';

update layoffs_new2
set industry = null
where industry = ''
;

select *
from layoffs_new2
where company = 'Airbnb';

select *
from layoffs_new2 t1
join layoffs_new2 t2
	on t1.company = t2.company
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_new2;

delete
from layoffs_new2
where total_laid_off is null
and percentage_laid_off is null;

alter table layoffs_new2
drop column row_num;


