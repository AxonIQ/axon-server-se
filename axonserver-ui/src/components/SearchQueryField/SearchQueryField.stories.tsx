import React from 'react';
import { SearchQueryField } from './SearchQueryField';

export default {
  title: 'Components/SearchQueryField',
  component: SearchQueryField,
};

export const Default = () => (
  <SearchQueryField
    onSubmit={(value) => console.log('hi from value -> ', value)}
  />
);
export const CustomPlaceholder = () => (
  <SearchQueryField
    placeholder="Please enter your query"
    onSubmit={(value) => console.log('hi from value -> ', value)}
  />
);
