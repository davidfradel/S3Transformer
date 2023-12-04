import { processCsvFile } from '../src/index';

describe('processCsvFile', () => {
  it('should process CSV file and return expected results', async () => {
    const segmentMappings = { 151: 'Technology', 1324: 'Health' }; // Example
    const results = await processCsvFile('./tests/sampleTest.csv', segmentMappings);

    expect(results).toBeDefined();
    expect(results.length).toBeGreaterThan(0);
  });

});
