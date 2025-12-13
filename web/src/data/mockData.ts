import type {
  Studio,
  Franchise,
  Film,
  Character,
  GenderByYear,
  GenderByRole,
  TalentStats,
} from '../types';

// Studios based on data directory structure
export const studios: Studio[] = [
  { id: 'wdas', name: 'Walt Disney Animation Studios', shortName: 'WDAS', franchiseIds: ['frozen', 'tangled', 'moana', 'zootopia', 'wreck-it-ralph', 'encanto'], filmCount: 62, characterCount: 1200 },
  { id: 'pixar', name: 'Pixar Animation Studios', shortName: 'Pixar', franchiseIds: ['toy-story', 'cars', 'incredibles', 'finding-nemo', 'monsters-inc', 'inside-out'], filmCount: 28, characterCount: 650 },
  { id: 'blue-sky', name: 'Blue Sky Studios', shortName: 'Blue Sky', franchiseIds: ['ice-age', 'rio'], filmCount: 13, characterCount: 280 },
  { id: 'disneytoon', name: 'DisneyToon Studios', shortName: 'DisneyToon', franchiseIds: ['tinker-bell', 'planes'], filmCount: 45, characterCount: 520 },
  { id: 'marvel-animation', name: 'Marvel Animation', shortName: 'Marvel', franchiseIds: ['spider-verse'], filmCount: 12, characterCount: 180 },
  { id: '20th-century', name: '20th Century Animation', shortName: '20th Century', franchiseIds: ['simpsons', 'bobs-burgers'], filmCount: 7, characterCount: 183 },
  { id: 'disney-interactive', name: 'Disney Interactive', shortName: 'Interactive', franchiseIds: ['kingdom-hearts', 'epic-mickey'], filmCount: 0, characterCount: 400 },
];

// Sample franchises
export const franchises: Franchise[] = [
  { id: 'frozen', name: 'Frozen', studioId: 'wdas', filmIds: ['frozen-2013', 'frozen-2-2019'] },
  { id: 'toy-story', name: 'Toy Story', studioId: 'pixar', filmIds: ['toy-story-1995', 'toy-story-2-1999', 'toy-story-3-2010', 'toy-story-4-2019'] },
  { id: 'ice-age', name: 'Ice Age', studioId: 'blue-sky', filmIds: ['ice-age-2002', 'ice-age-2-2006', 'ice-age-3-2009', 'ice-age-4-2012', 'ice-age-5-2016'] },
  { id: 'finding-nemo', name: 'Finding Nemo', studioId: 'pixar', filmIds: ['finding-nemo-2003', 'finding-dory-2016'] },
  { id: 'incredibles', name: 'The Incredibles', studioId: 'pixar', filmIds: ['incredibles-2004', 'incredibles-2-2018'] },
];

// Sample films
export const films: Film[] = [
  { id: 'frozen-2013', title: 'Frozen', year: 2013, franchiseId: 'frozen', studioId: 'wdas', characterCount: 15 },
  { id: 'frozen-2-2019', title: 'Frozen II', year: 2019, franchiseId: 'frozen', studioId: 'wdas', characterCount: 15 },
  { id: 'toy-story-1995', title: 'Toy Story', year: 1995, franchiseId: 'toy-story', studioId: 'pixar', characterCount: 30 },
  { id: 'toy-story-2-1999', title: 'Toy Story 2', year: 1999, franchiseId: 'toy-story', studioId: 'pixar', characterCount: 23 },
  { id: 'toy-story-3-2010', title: 'Toy Story 3', year: 2010, franchiseId: 'toy-story', studioId: 'pixar', characterCount: 33 },
  { id: 'toy-story-4-2019', title: 'Toy Story 4', year: 2019, franchiseId: 'toy-story', studioId: 'pixar', characterCount: 27 },
  { id: 'finding-nemo-2003', title: 'Finding Nemo', year: 2003, franchiseId: 'finding-nemo', studioId: 'pixar', characterCount: 18 },
  { id: 'the-lion-king-1994', title: 'The Lion King', year: 1994, franchiseId: 'lion-king', studioId: 'wdas', characterCount: 12 },
  { id: 'moana-2016', title: 'Moana', year: 2016, franchiseId: 'moana', studioId: 'wdas', characterCount: 10 },
  { id: 'coco-2017', title: 'Coco', year: 2017, franchiseId: 'coco', studioId: 'pixar', characterCount: 15 },
];

// Sample characters
export const characters: Character[] = [
  { id: 'elsa', name: 'Elsa', role: 'protagonist', voiceActor: 'Idina Menzel', species: 'human', gender: 'female', filmIds: ['frozen-2013', 'frozen-2-2019'], franchiseId: 'frozen', studioId: 'wdas' },
  { id: 'anna', name: 'Anna', role: 'protagonist', voiceActor: 'Kristen Bell', species: 'human', gender: 'female', filmIds: ['frozen-2013', 'frozen-2-2019'], franchiseId: 'frozen', studioId: 'wdas' },
  { id: 'olaf', name: 'Olaf', role: 'sidekick', voiceActor: 'Josh Gad', species: 'snowman', gender: 'male', filmIds: ['frozen-2013', 'frozen-2-2019'], franchiseId: 'frozen', studioId: 'wdas' },
  { id: 'woody', name: 'Woody', role: 'protagonist', voiceActor: 'Tom Hanks', species: 'toy', gender: 'male', filmIds: ['toy-story-1995', 'toy-story-2-1999', 'toy-story-3-2010', 'toy-story-4-2019'], franchiseId: 'toy-story', studioId: 'pixar' },
  { id: 'buzz', name: 'Buzz Lightyear', role: 'deuteragonist', voiceActor: 'Tim Allen', species: 'toy', gender: 'male', filmIds: ['toy-story-1995', 'toy-story-2-1999', 'toy-story-3-2010', 'toy-story-4-2019'], franchiseId: 'toy-story', studioId: 'pixar' },
  { id: 'jessie', name: 'Jessie', role: 'hero', voiceActor: 'Joan Cusack', species: 'toy', gender: 'female', filmIds: ['toy-story-2-1999', 'toy-story-3-2010', 'toy-story-4-2019'], franchiseId: 'toy-story', studioId: 'pixar' },
  { id: 'nemo', name: 'Nemo', role: 'protagonist', voiceActor: 'Alexander Gould', species: 'clownfish', gender: 'male', filmIds: ['finding-nemo-2003'], franchiseId: 'finding-nemo', studioId: 'pixar' },
  { id: 'dory', name: 'Dory', role: 'sidekick', voiceActor: 'Ellen DeGeneres', species: 'blue tang', gender: 'female', filmIds: ['finding-nemo-2003', 'finding-dory-2016'], franchiseId: 'finding-nemo', studioId: 'pixar' },
  { id: 'simba', name: 'Simba', role: 'protagonist', voiceActor: 'Matthew Broderick', species: 'lion', gender: 'male', filmIds: ['the-lion-king-1994'], franchiseId: 'lion-king', studioId: 'wdas' },
  { id: 'moana', name: 'Moana', role: 'protagonist', voiceActor: "Auli'i Cravalho", species: 'human', gender: 'female', filmIds: ['moana-2016'], franchiseId: 'moana', studioId: 'wdas' },
  { id: 'maui', name: 'Maui', role: 'deuteragonist', voiceActor: 'Dwayne Johnson', species: 'demigod', gender: 'male', filmIds: ['moana-2016'], franchiseId: 'moana', studioId: 'wdas' },
  { id: 'miguel', name: 'Miguel Rivera', role: 'protagonist', voiceActor: 'Anthony Gonzalez', species: 'human', gender: 'male', filmIds: ['coco-2017'], franchiseId: 'coco', studioId: 'pixar' },
];

// Gender representation by year (mocked aggregation)
export const genderByYear: GenderByYear[] = [
  { year: 1995, male: 24, female: 6, other: 0, total: 30 },
  { year: 1998, male: 18, female: 8, other: 1, total: 27 },
  { year: 2000, male: 20, female: 10, other: 2, total: 32 },
  { year: 2003, male: 22, female: 12, other: 1, total: 35 },
  { year: 2006, male: 19, female: 14, other: 2, total: 35 },
  { year: 2010, male: 18, female: 16, other: 3, total: 37 },
  { year: 2013, male: 15, female: 18, other: 2, total: 35 },
  { year: 2016, male: 16, female: 20, other: 4, total: 40 },
  { year: 2019, male: 18, female: 22, other: 5, total: 45 },
  { year: 2022, male: 17, female: 24, other: 6, total: 47 },
];

// Gender by role (mocked)
export const genderByRole: GenderByRole[] = [
  { role: 'protagonist', male: 35, female: 28, other: 2 },
  { role: 'deuteragonist', male: 30, female: 22, other: 1 },
  { role: 'villain', male: 42, female: 12, other: 3 },
  { role: 'antagonist', male: 38, female: 15, other: 2 },
  { role: 'sidekick', male: 45, female: 20, other: 8 },
  { role: 'comic_relief', male: 52, female: 18, other: 5 },
  { role: 'supporting', male: 120, female: 85, other: 15 },
  { role: 'minor', male: 200, female: 150, other: 30 },
];

// Most appearances across media (films + games)
export const mostAppearances = [
  { name: 'Mickey Mouse', appearances: 28, films: 12, games: 16, studio: 'WDAS' },
  { name: 'Donald Duck', appearances: 22, films: 10, games: 12, studio: 'WDAS' },
  { name: 'Goofy', appearances: 20, films: 9, games: 11, studio: 'WDAS' },
  { name: 'Sora', appearances: 14, films: 0, games: 14, studio: 'Interactive' },
  { name: 'Woody', appearances: 12, films: 5, games: 7, studio: 'Pixar' },
  { name: 'Buzz Lightyear', appearances: 12, films: 5, games: 7, studio: 'Pixar' },
  { name: 'Jack Sparrow', appearances: 10, films: 5, games: 5, studio: 'WDAS' },
  { name: 'Simba', appearances: 9, films: 3, games: 6, studio: 'WDAS' },
  { name: 'Stitch', appearances: 9, films: 4, games: 5, studio: 'WDAS' },
  { name: 'Elsa', appearances: 8, films: 2, games: 6, studio: 'WDAS' },
];

// Species breakdown across all characters
export const speciesBreakdown = [
  { species: 'Human', count: 485, percentage: 32 },
  { species: 'Animal', count: 380, percentage: 25 },
  { species: 'Toy', count: 165, percentage: 11 },
  { species: 'Monster', count: 120, percentage: 8 },
  { species: 'Fish/Sea', count: 95, percentage: 6 },
  { species: 'Fairy/Magical', count: 85, percentage: 6 },
  { species: 'Robot/Machine', count: 75, percentage: 5 },
  { species: 'Insect', count: 55, percentage: 4 },
  { species: 'Other', count: 50, percentage: 3 },
];

// Sequel character retention rates by franchise
export const sequelRetention = [
  { franchise: 'Toy Story', original: 30, retained: 12, retention: 40, sequels: 3 },
  { franchise: 'Frozen', original: 15, retained: 8, retention: 53, sequels: 1 },
  { franchise: 'Ice Age', original: 12, retained: 4, retention: 33, sequels: 4 },
  { franchise: 'Cars', original: 18, retained: 6, retention: 33, sequels: 2 },
  { franchise: 'Finding Nemo', original: 18, retained: 5, retention: 28, sequels: 1 },
  { franchise: 'Incredibles', original: 15, retained: 10, retention: 67, sequels: 1 },
  { franchise: 'Monsters Inc', original: 14, retained: 4, retention: 29, sequels: 1 },
  { franchise: 'Shrek', original: 12, retained: 8, retention: 67, sequels: 3 },
  { franchise: 'Kung Fu Panda', original: 14, retained: 6, retention: 43, sequels: 3 },
  { franchise: 'Despicable Me', original: 15, retained: 9, retention: 60, sequels: 3 },
];

// Cross-media stars (characters appearing in both films AND games)
export const crossMediaStars = [
  { name: 'Mickey Mouse', films: 12, games: 16, origin: 'WDAS', khAppearances: 8 },
  { name: 'Donald Duck', films: 10, games: 12, origin: 'WDAS', khAppearances: 8 },
  { name: 'Goofy', films: 9, games: 11, origin: 'WDAS', khAppearances: 8 },
  { name: 'Jack Sparrow', films: 5, games: 5, origin: 'Live Action', khAppearances: 2 },
  { name: 'Simba', films: 3, games: 6, origin: 'WDAS', khAppearances: 3 },
  { name: 'Aladdin', films: 3, games: 8, origin: 'WDAS', khAppearances: 4 },
  { name: 'Ariel', films: 2, games: 5, origin: 'WDAS', khAppearances: 2 },
  { name: 'Hercules', films: 1, games: 4, origin: 'WDAS', khAppearances: 3 },
  { name: 'Buzz Lightyear', films: 5, games: 7, origin: 'Pixar', khAppearances: 1 },
  { name: 'Woody', films: 5, games: 7, origin: 'Pixar', khAppearances: 1 },
  { name: 'Stitch', films: 4, games: 5, origin: 'WDAS', khAppearances: 2 },
  { name: 'Peter Pan', films: 2, games: 4, origin: 'WDAS', khAppearances: 3 },
  { name: 'Maleficent', films: 2, games: 5, origin: 'WDAS', khAppearances: 5 },
  { name: 'Cloud Strife', films: 0, games: 3, origin: 'Square Enix', khAppearances: 3 },
];

// Prolific voice actors (most characters voiced)
export const prolificActors = [
  { name: 'John Ratzenberger', characters: 24, films: 24, primaryStudio: 'Pixar' },
  { name: 'Frank Welker', characters: 18, films: 15, primaryStudio: 'Various' },
  { name: 'Jim Cummings', characters: 16, films: 22, primaryStudio: 'WDAS' },
  { name: 'Tress MacNeille', characters: 14, films: 12, primaryStudio: 'Various' },
  { name: 'Rob Paulsen', characters: 12, films: 10, primaryStudio: 'Various' },
  { name: 'Grey DeLisle', characters: 11, films: 9, primaryStudio: 'Various' },
  { name: 'Tom Kenny', characters: 10, films: 8, primaryStudio: 'Various' },
  { name: 'Maurice LaMarche', characters: 9, films: 11, primaryStudio: 'Various' },
  { name: 'Carlos Alazraqui', characters: 8, films: 7, primaryStudio: 'Various' },
  { name: 'Dee Bradley Baker', characters: 8, films: 10, primaryStudio: 'Various' },
];

// Top voice talents (mocked earnings until box office lands)
export const topTalents: TalentStats[] = [
  { name: 'Tom Hanks', filmCount: 4, characterCount: 1, studios: ['Pixar'], estimatedEarnings: 3200000000 },
  { name: 'Tim Allen', filmCount: 4, characterCount: 1, studios: ['Pixar'], estimatedEarnings: 3200000000 },
  { name: 'Idina Menzel', filmCount: 2, characterCount: 1, studios: ['WDAS'], estimatedEarnings: 2800000000 },
  { name: 'Josh Gad', filmCount: 2, characterCount: 1, studios: ['WDAS'], estimatedEarnings: 2800000000 },
  { name: 'Ellen DeGeneres', filmCount: 2, characterCount: 1, studios: ['Pixar'], estimatedEarnings: 2100000000 },
  { name: 'John Ratzenberger', filmCount: 24, characterCount: 24, studios: ['Pixar'], estimatedEarnings: 15000000000 },
  { name: 'Kristen Bell', filmCount: 2, characterCount: 1, studios: ['WDAS'], estimatedEarnings: 2800000000 },
  { name: 'Dwayne Johnson', filmCount: 2, characterCount: 1, studios: ['WDAS'], estimatedEarnings: 1400000000 },
  { name: 'Eddie Murphy', filmCount: 4, characterCount: 2, studios: ['WDAS', 'DreamWorks'], estimatedEarnings: 1800000000 },
  { name: 'Mike Myers', filmCount: 4, characterCount: 1, studios: ['DreamWorks'], estimatedEarnings: 3500000000 },
];

// Data access functions
export function getStudioById(id: string): Studio | undefined {
  return studios.find((s) => s.id === id);
}

export function getFilmById(id: string): Film | undefined {
  return films.find((f) => f.id === id);
}

export function getCharacterById(id: string): Character | undefined {
  return characters.find((c) => c.id === id);
}

export function getCharactersByFilm(filmId: string): Character[] {
  return characters.filter((c) => c.filmIds.includes(filmId));
}

export function getCharactersByStudio(studioId: string): Character[] {
  return characters.filter((c) => c.studioId === studioId);
}

export function getFilmsByStudio(studioId: string): Film[] {
  return films.filter((f) => f.studioId === studioId);
}

export function getFranchisesByStudio(studioId: string): Franchise[] {
  return franchises.filter((f) => f.studioId === studioId);
}

export function searchCharacters(query: string): Character[] {
  const q = query.toLowerCase();
  return characters.filter(
    (c) =>
      c.name.toLowerCase().includes(q) ||
      c.voiceActor?.toLowerCase().includes(q) ||
      c.species.toLowerCase().includes(q)
  );
}
