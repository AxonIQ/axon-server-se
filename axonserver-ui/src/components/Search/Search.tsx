import React from 'react';
import InputBase from '@material-ui/core/InputBase';
import SearchIcon from '@material-ui/icons/Search';
import './search.scss';

export const Search = () => (
  <div className="search__wrapper">
    <div className="search__icon">
      <SearchIcon fontSize="large" />
    </div>
    <InputBase
      placeholder="Search"
      classes={{
        root: 'search',
        input: 'search__input',
      }}
      inputProps={{ 'aria-label': 'search' }}
    />
  </div>
);
