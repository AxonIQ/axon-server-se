import React from 'react';
import { SearchRowDetails } from './SearchRowDetails';

export default {
  title: 'Components/SearchRowDetails',
  component: SearchRowDetails,
};

export const Default = () => (
  <SearchRowDetails
    dataItem={{
      aggregateSequenceNumber: 0,
      metaData:
        '{traceId=065dd789-f091-49fc-b32a-1aba24d5313e, correlationId=065dd789-f091-49fc-b32a-1aba24d5313e}',
      payloadType: 'io.axoniq.demo.giftcard.api.IssuedEvt',
      payloadRevision: '',
      aggregateIdentifier: '6CCB5D6B-65',
      payloadData:
        '<io.axoniq.demo.giftcard.api.IssuedEvt><id>6CCB5D6B-65</id><amount>100</amount></io.axoniq.demo.giftcard.api.IssuedEvt>',
      eventIdentifier: '4e646b18-276c-4029-be56-d3a0e681df22',
      token: 88,
      aggregateType: 'GiftCard',
      timestamp: '2020-11-03T12:00:50.506Z',
    }}
  />
);
