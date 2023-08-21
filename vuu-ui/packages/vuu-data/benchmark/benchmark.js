import { createArray } from '/Users/joachim/Desktop/UBS/vuu-my-copy/vuu/vuu-ui/packages/vuu-data/src/array-data-source/generate-data-utils.ts';
import { aggregateData} from '/Users/joachim/Desktop/UBS/vuu-my-copy/vuu/vuu-ui/packages/vuu-data/src/array-data-source/aggregate-utils.ts';


const Benchmark = require('benchmark');

var suite = new Benchmark.Suite;

// add tests
suite.add('Test Generate Data', () => {
    createArray(1000);
});

suite.add('Test Aggregate Data Sum', () => {
    aggregateData(generateData(1000), [{field: 'age', aggregate: 'sum'}]);
});

// Configure the environments
suite
  .on('start', () => {
    // Set up environment-specific settings here
    createArray(10000);
  })
  .on('complete', () => {
    // Clean up environment-specific settings here
  });

suite.on('complete', () => {
    const slowest = suite.filter('slowest').map('name');
    const fastest = suite.filter('fastest').map('name');
    
    const totalTime = suite.reduce((total, benchmark) => total + benchmark.times.elapsed, 0);
    const averageTime = totalTime / suite.length;
  
    console.log(`Slowest: ${slowest}`);
    console.log(`Fastest: ${fastest}`);
    console.log(`Average time: ${averageTime.toFixed(2)} ms`);
  });

suite.run();