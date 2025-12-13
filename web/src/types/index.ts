// Core domain types based on data schema

export type Role =
  | 'protagonist'
  | 'deuteragonist'
  | 'hero'
  | 'villain'
  | 'antagonist'
  | 'henchman'
  | 'sidekick'
  | 'mentor'
  | 'love_interest'
  | 'comic_relief'
  | 'ally'
  | 'supporting'
  | 'minor';

export type Gender = 'male' | 'female' | 'unknown' | 'n/a' | 'mixed';

export interface Character {
  id: string;
  name: string;
  role: Role;
  voiceActor: string | null;
  species: string;
  gender: Gender;
  notes?: string;
  filmIds: string[];
  franchiseId: string;
  studioId: string;
}

export interface Film {
  id: string;
  title: string;
  year: number;
  franchiseId: string;
  studioId: string;
  characterCount: number;
}

export interface Franchise {
  id: string;
  name: string;
  studioId: string;
  filmIds: string[];
}

export interface Studio {
  id: string;
  name: string;
  shortName: string;
  franchiseIds: string[];
  filmCount: number;
  characterCount: number;
}

export interface VoiceTalent {
  id: string;
  name: string;
  characterIds: string[];
  filmCount: number;
  // Will add earnings when box office data lands
  estimatedEarnings?: number;
}

// Aggregation types for visualizations
export interface GenderByYear {
  year: number;
  male: number;
  female: number;
  other: number;
  total: number;
}

export interface GenderByRole {
  role: Role;
  male: number;
  female: number;
  other: number;
}

export interface TalentStats {
  name: string;
  filmCount: number;
  characterCount: number;
  studios: string[];
  // Placeholder until box office data
  estimatedEarnings: number;
}

// Auth types
export interface User {
  id: string;
  name: string;
  email: string;
  role: 'admin' | 'viewer';
}
