import React from 'react';
import { BrowserRouter as Router } from 'react-router-dom';
import '../../app.scss';
import { Navigation } from './Navigation';

export default {
  title: 'Components/Navigation',
  component: Navigation,
};

export const Default = () => (
  <Router>
    <div className="app-root">
      <Navigation />
    </div>
  </Router>
);
export const SearchActive = () => (
  <Router>
    <div className="app-root">
      <Navigation active="search" />
    </div>
  </Router>
);
