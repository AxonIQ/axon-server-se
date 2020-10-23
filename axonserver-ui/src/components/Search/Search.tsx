import React, { useState } from 'react';
import InputBase from '@material-ui/core/InputBase';
import SearchIcon from '@material-ui/icons/Search';
import './search.scss';

type SearchProps = {
  onSubmit?: (value: string) => void;
};
export const Search = (props: SearchProps) => {
  const [inputValue, setInputValue] = useState('');

  return (
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
        value={inputValue}
        onChange={(event) => setInputValue(event.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && props.onSubmit) {
            props.onSubmit(inputValue);
          }
        }}
        inputProps={{ 'aria-label': 'search' }}
      />
    </div>
  );
};
