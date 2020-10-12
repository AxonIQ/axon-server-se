import React from 'react';
import { Navigation } from './Navigation';

export default {
    title: 'Components/Navigation',
    component: Navigation,
};

export const Primary = () => <Navigation />;
export const SearchActive = () => <Navigation active="search"/>
