import React from 'react';
import { SearchInput } from './SearchInput';

export default {
  title: 'Components/SearchInput',
  component: SearchInput,
};

export const Default = () => (
  <SearchInput onSubmit={(value) => console.log('hi from value -> ', value)} />
);
export const CustomPlaceholder = () => (
  <SearchInput
    placeholder="Please enter your query"
    onSubmit={(value) => console.log('hi from value -> ', value)}
  />
);
