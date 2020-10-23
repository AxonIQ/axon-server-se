import React from 'react';
import { SearchInput } from './SearchInput';

export default {
  title: 'Components/SearchInput',
  component: SearchInput,
};

export const Default = () => (
  <SearchInput onSubmit={(value) => console.log('hi from value -> ', value)} />
);
