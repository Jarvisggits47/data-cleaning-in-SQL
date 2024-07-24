use project;
show tables;
SELECT *FROM  layoffs;
-- first thing we want to do is create a another table. This is the one we will work in and clean the data. 
-- We want a table with the raw data in case something happens

CREATE TABLE layoffs_1
like layoffs;
INSERT INTO layoffs_1
-- --copy the whole data into new table
-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

-- 1. Remove Duplicates

# First let's check for duplicates 
SELECT *FROM layoffs_1;
SELECT *,
ROW_NUMBER()OVER(PARTITION BY company,industry,stage,funds_raised_millions,total_laid_off,percentage_laid_off,country,`date`) AS row_num
FROM layoffs_1;
-- to get duplicates
WITH duplicate_cte AS(SELECT *,
ROW_NUMBER()OVER(PARTITION BY company,stage,funds_raised_millions,location,industry,total_laid_off,percentage_laid_off,country,`date`) AS row_num
FROM layoffs_1
)
SELECT *FROM 
duplicate_cte
WHERE row_num>1;
-- let's just look at oda to confirm

SELECT *FROM layoffs_1
WHERE company='Casper';

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!
CREATE TABLE `layoffs_2` (
  `company` text DEFAULT NULL,
  `location` text DEFAULT NULL,
  `industry` text DEFAULT NULL,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text DEFAULT NULL,
  `date` text DEFAULT NULL,
  `stage` text DEFAULT NULL,
  `country` text DEFAULT NULL,
  `funds_raised_millions` int(11) DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

INSERT INTO layoffs_2
SELECT *,
ROW_NUMBER()OVER(PARTITION BY company,stage,funds_raised_millions,location,industry,total_laid_off,percentage_laid_off,country,`date`) AS row_num
FROM layoffs_1;
-- now that we have this we can delete rows were row_num is greater than 2
DELETE FROM layoffs_2
WHERE row_num>1;
-- 2. Standardize Data
-- if we look at industry it looks like we have some null and empty rows, let's take a look at these

SELECT *FROM layoffs_2;
SELECT company,TRIM(company)
FROM layoffs_2;
UPDATE layoffs_2
-- trim method tooks off the white spaces from the ...
SET company=TRIM(company);
SELECT DISTINCT industry
FROM layoffs_2;
-- Standardizing the data
-- I also noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto

SELECT *FROM layoffs_2
WHERE industry LIKE 'Crypto%';
UPDATE layoffs_2
SET industry ='Crypto'
WHERE  industry LIKE 'Crypto%';
SELECT DISTINCT country
FROM layoffs_2
ORDER BY 1;
SELECT *FROM 
layoffs_2
WHERE country like 'United States%'
ORDER BY 1;
-- everything looks good except apparently we have some "United States" and some "United States." 
-- with a period at the end. Let's standardize this.
SELECT DISTINCT country,TRIM( TRAILING '.' FROM  country)
FROM layoffs_2
ORDER BY 1;
-- now the issue gone
UPDATE layoffs_2
SET country =TRIM( TRAILING '.' FROM  country)
WHERE country LIKE 'United States%';
-- Let's also fix the date columns:
SELECT *FROM layoffs_2;
-- we can use str to date to update this field
SELECT `date`,
STR_TO_DATE(`date`,'%m/%d/%Y') AS format_date
FROM layoffs_2;
UPDATE layoffs_2
SET `date` =STR_TO_DATE(`date`,'%m/%d/%Y') ;
SELECT *from layoffs_2;
-- now we can convert the data type properly
ALTER TABLE layoffs_2
MODIFY COLUMN `date` DATE;

-- 3. Look at Null Values

-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal. I don't think I want to change that
-- I like having them null because it makes it easier for calculations during the EDA phase

-- so there isn't anything I want to change with the null values

-- 4. remove any columns and rows we need to
SELECT *FROM layoffs_2
WHERE  total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- few blanks are there 
SELECT *
FROM layoffs_2
WHERE industry=''
OR industry IS NULL;
-- we should set the blanks to nulls since those are typically easier to work with
UPDATE layoffs_2
SET industry=NULL
WHERE industry='';

SELECT *FROM layoffs_2 l1
JOIN layoffs_2 l2
ON l1.company=l2.company
WHERE(l1.industry IS NULL OR l1.industry ='')
AND l2.industry IS  NOT NULL;
-- now if we check those are all null
SELECT l1.industry,l2.industry
FROM layoffs_2 l1
JOIN layoffs_2 l2
ON l1.company=l2.company
WHERE(l1.industry IS NULL OR l1.industry ='')
AND l2.industry IS  NOT NULL;
-- now we need to populate those nulls if possible
UPDATE layoffs_2 l1
JOIN layoffs_2 l2
ON l1.company=l2.company
SET l1.industry =l2.industry
WHERE (l1.industry IS NULL )
AND l2.industry IS  NOT NULL;

SELECT *FROM layoffs_2
WHERE company='Airbnb';
SELECT *FROM layoffs_2;

SELECT *FROM layoffs_2
WHERE  total_laid_off IS NULL
AND percentage_laid_off IS NULL;
-- Delete Useless data we can't really use
DELETE 
FROM layoffs_2
WHERE  total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE  layoffs_2
DROP COLUMN row_num;
SELECT *FROM layoffs_2;

