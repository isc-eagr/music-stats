-- Fix city/state entries in Artist country field
-- Maps cities and states to their proper countries

-- US States and Cities -> United States
UPDATE Artist SET country = 'United States' WHERE country = 'Alabama';
UPDATE Artist SET country = 'United States' WHERE country = 'Alpharetta';
UPDATE Artist SET country = 'United States' WHERE country = 'Ann Arbor';
UPDATE Artist SET country = 'United States' WHERE country = 'Atlanta';
UPDATE Artist SET country = 'United States' WHERE country = 'Brooklyn';
UPDATE Artist SET country = 'United States' WHERE country = 'Carolina';
UPDATE Artist SET country = 'United States' WHERE country = 'Chicago';
UPDATE Artist SET country = 'United States' WHERE country = 'Denison';
UPDATE Artist SET country = 'United States' WHERE country = 'Detroit';
UPDATE Artist SET country = 'United States' WHERE country = 'Florida';
UPDATE Artist SET country = 'United States' WHERE country = 'Hawaii';
UPDATE Artist SET country = 'United States' WHERE country = 'Jacksonville';
UPDATE Artist SET country = 'United States' WHERE country = 'Lemoore';
UPDATE Artist SET country = 'United States' WHERE country = 'Los Angeles';
UPDATE Artist SET country = 'United States' WHERE country = 'Manhattan';
UPDATE Artist SET country = 'United States' WHERE country = 'Miami';
UPDATE Artist SET country = 'United States' WHERE country = 'Nashville';
UPDATE Artist SET country = 'United States' WHERE country = 'New Jersey';
UPDATE Artist SET country = 'United States' WHERE country = 'New York';
UPDATE Artist SET country = 'United States' WHERE country = 'Saddle River';
UPDATE Artist SET country = 'United States' WHERE country = 'San Francisco';

-- Mexican Cities -> Mexico
UPDATE Artist SET country = 'Mexico' WHERE country = 'Chihuahua';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Ciudad Juárez';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Ciudad de México';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Culiacán';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Guadalajara';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Los Mochis';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Mazatlan';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Monterrey';

-- Colombian Cities -> Colombia
UPDATE Artist SET country = 'Colombia' WHERE country = 'Cali';
UPDATE Artist SET country = 'Colombia' WHERE country = 'Medellín';
UPDATE Artist SET country = 'Colombia' WHERE country = 'Santa Marta';

-- Venezuelan Cities -> Venezuela
UPDATE Artist SET country = 'Venezuela' WHERE country = 'Caracas';

-- Cuban Cities -> Cuba
UPDATE Artist SET country = 'Cuba' WHERE country = 'La Habana';

-- Spanish Cities/Regions -> Spain
UPDATE Artist SET country = 'Spain' WHERE country = 'Catalunya';
UPDATE Artist SET country = 'Spain' WHERE country = 'Las Palmas de Gran Canaria';
UPDATE Artist SET country = 'Spain' WHERE country = 'Madrid';

-- UK/English Cities -> United Kingdom
UPDATE Artist SET country = 'United Kingdom' WHERE country = 'England';
UPDATE Artist SET country = 'United Kingdom' WHERE country = 'Liverpool';
UPDATE Artist SET country = 'United Kingdom' WHERE country = 'London';
UPDATE Artist SET country = 'United Kingdom' WHERE country = 'Manchester';

-- German Cities -> Germany
UPDATE Artist SET country = 'Germany' WHERE country = 'Leipzig';

-- French Cities -> France
UPDATE Artist SET country = 'France' WHERE country = 'Paris';

-- Brazilian Cities/States -> Brazil
UPDATE Artist SET country = 'Brazil' WHERE country = 'Mato Grosso do Sul';
UPDATE Artist SET country = 'Brazil' WHERE country = 'Rio de Janeiro';

-- Canadian Cities -> Canada
UPDATE Artist SET country = 'Canada' WHERE country = 'Mississauga';

-- Australian Cities -> Australia
UPDATE Artist SET country = 'Australia' WHERE country = 'Perth';

-- Chilean Cities -> Chile
UPDATE Artist SET country = 'Chile' WHERE country = 'Santiago';

-- Ecuadorian Cities -> Ecuador
UPDATE Artist SET country = 'Ecuador' WHERE country = 'Esmeraldas';

-- Puerto Rican Cities -> Puerto Rico (already a territory/country name)
UPDATE Artist SET country = 'Puerto Rico' WHERE country = 'Patillas';
UPDATE Artist SET country = 'Puerto Rico' WHERE country = 'Santurce';
UPDATE Artist SET country = 'Puerto Rico' WHERE country = 'Yabucoa';

-- More Mexican States/Cities -> Mexico
UPDATE Artist SET country = 'Mexico' WHERE country = 'Sonora';
UPDATE Artist SET country = 'Mexico' WHERE country = 'Veracruz';

-- More US Cities -> United States
UPDATE Artist SET country = 'United States' WHERE country = 'Temecula';
UPDATE Artist SET country = 'United States' WHERE country = 'The Bronx';

-- More Canadian Cities -> Canada
UPDATE Artist SET country = 'Canada' WHERE country = 'Toronto';

-- More Australian Cities -> Australia
UPDATE Artist SET country = 'Australia' WHERE country = 'Sydney';

-- More Chilean Cities -> Chile
UPDATE Artist SET country = 'Chile' WHERE country = 'Talca';

-- Unknown/Invalid country codes - setting to NULL for manual review
-- XG and XW appear to be invalid ISO codes or placeholders
UPDATE Artist SET country = NULL WHERE country = 'XG';
UPDATE Artist SET country = NULL WHERE country = 'XW';

-- Ambiguous cases that need manual review (Jersey could be New Jersey or the island)
-- Leaving Jersey as-is since it could refer to the island nation
-- UPDATE Artist SET country = 'United Kingdom' WHERE country = 'Jersey';

-- Río Grande could be multiple places, leaving as-is for manual review
-- UPDATE Artist SET country = 'United States' WHERE country = 'Río Grande';

-- Display summary of changes
SELECT 'Fixed city/state entries to proper countries' as status;
