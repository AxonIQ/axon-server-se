import InputBase from '@material-ui/core/InputBase';
import SearchIcon from '@material-ui/icons/Search';
import React, { useState } from 'react';
import './search-input.scss';

type SearchInputProps = {
  multiline?: boolean;
  placeholder?: string;
  value?: string;
  onSubmit?: (value: string) => void;
};
export const SearchInput = (props: SearchInputProps) => {
  const [inputValue, setInputValue] = useState(props.value ? props.value : '');

  return (
    <div className="search-input__wrapper">
      <div className="search-input__icon">
        <SearchIcon fontSize="large" />
      </div>
      <InputBase
        multiline={props.multiline}
        placeholder={props.placeholder ? props.placeholder : 'Search'}
        classes={{
          root: 'search-input',
          input: 'search-input__input',
        }}
        value={inputValue}
        onChange={(event) => setInputValue(event.target.value)}
        onKeyDown={(e) => {
          if (e.key === 'Enter' && props.onSubmit) {
            e.preventDefault();
            props.onSubmit(inputValue);
          }
        }}
        inputProps={{ 'aria-label': 'search' }}
      />
    </div>
  );
};
