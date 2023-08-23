import {
  VuuAggregation,
  VuuGroupBy,
  VuuRange,
  VuuRowDataItemType,
  VuuSort,
} from "@finos/vuu-protocol-types";
import {
  collapseGroup,
  expandGroup,
  GroupMap,
  groupRows,
  KeyList,
} from "./group-utils";

import { DataSourceRow } from "@finos/vuu-data-types";
import { ColumnMap } from "@finos/vuu-utils";
import { group } from "console";

export const count = (arr: any[]) => arr.length;

export const aggregateData = (
  aggregations: VuuAggregation[],
  targetData: readonly DataSourceRow[],
  groupBy: VuuGroupBy,
  leafData: readonly DataSourceRow[],
  columnMap: ColumnMap,
  groupMap: GroupMap
) => {
  const aggColumn = getAggColumn(columnMap, aggregations);
  const aggType = aggregations[aggregations.length - 1].aggType;
  const groupIndices = groupBy.map<number>((column) => columnMap[column]);
  const benchmarkcount = 1000 ;

  switch (aggType) {
    case 1:
      let countsum = 0;
      let avgsum = [];
      while (countsum < benchmarkcount) {
        const t0 = performance.now();
        const summ = aggregateSum(
          groupMap,
          leafData,
          columnMap,
          aggregations,
          targetData,
          groupIndices
        );
        const t1 = performance.now();
        avgsum.push(t1 - t0);
        countsum += 1;
      }
      console.log("average of sum", avgsum.reduce((a, b) => a + b) / avgsum.length);
      return 0;
    case 2:
      let countavg = 0;
      let avgavg = [];
      while (countavg < benchmarkcount) {
        const t0 = performance.now();
        const avg = aggregateAverage(
          groupMap,
          leafData,
          columnMap,
          aggregations,
          targetData,
          groupIndices
        );
        const t1 = performance.now();
        avgavg.push(t1 - t0);
        countavg += 1;
      }
      console.log("average of average", avgavg.reduce((a, b) => a + b) / avgavg.length);
      return 0;
    case 3:
      let countcount = 0;
      let avgcount = [];
      while (countcount < benchmarkcount) {
        const t0 = performance.now();
        const count = aggregateCount(
          groupMap,
          columnMap,
          aggregations,
          targetData,
          groupIndices
        );
        const t1 = performance.now();
        avgcount.push(t1 - t0);
        countcount += 1;
      }
      console.log("average of count", avgcount.reduce((a, b) => a + b) / avgcount.length);
      return 0;
    case 4:
      let counthigh = 0;
      let avghigh = [];
      while (counthigh < benchmarkcount) {
        const t0 = performance.now();
        const high = aggregateHigh(
          groupMap,
          leafData,
          columnMap,
          aggregations,
          targetData,
          groupIndices
        );
        const t1 = performance.now();
        avghigh.push(t1 - t0);
        counthigh += 1;
      }
      console.log("average of high", avghigh.reduce((a, b) => a + b) / avghigh.length);
      return 0;
    case 5:
      let countlow = 0;
      let avglow = [];
      while (countlow < benchmarkcount) {
        const t0 = performance.now();
        const low = aggregateLow(
          groupMap,
          leafData,
          columnMap,
          aggregations,
          targetData,
          groupIndices
        );
        const t1 = performance.now();
        avglow.push(t1 - t0);
        countlow += 1;
      }
      console.log("average of low", avglow.reduce((a, b) => a + b) / avglow.length);
      return 0;
    case 6:
      let countdistinct = 0;
      let avgdistinct = [];
      while (countdistinct < benchmarkcount) {
        const t0 = performance.now();
        const distinct = aggregateDistinct(
          groupMap,
          leafData,
          columnMap,
          aggregations,
          targetData,
          groupIndices
        );
        const t1 = performance.now();
        avgdistinct.push(t1 - t0);
        countdistinct += 1;
      }
      console.log("average of distinct", avgdistinct.reduce((a, b) => a + b) / avgdistinct.length);
      return 0;
  }

};

function aggregateCount(
  groupMap: GroupMap,
  columnMap: ColumnMap,
  aggregations: VuuAggregation[],
  targetData: readonly DataSourceRow[],
  groupIndices : number[],
): { [key: string]: number } {
  const counts: { [key: string]: number } = {};
  const aggColumn = getAggColumn(columnMap, aggregations);

  function countRecursive(map: GroupMap | KeyList) {
    if (Array.isArray(map)) {
      return map.length;
    } else {
      let count = 0;
      for (const key in map) {
        count += 1 + countRecursive(map[key]);
      }
      return count;
    }
  }

  for (const key in groupMap) {
    const count = countRecursive(groupMap[key]);
    counts[key] = count;
  }

  for (let index = 0; index < targetData.length; index++) {
    for (const key in counts) {
      if (targetData[index][groupIndices[0]] === key) {
        targetData[index][aggColumn] = counts[key];
      }
    }
  }

  return counts;
}

function getAggColumn(columnMap: ColumnMap, aggregations: VuuAggregation[]) {
  const columnName = aggregations[aggregations.length - 1].column;
  const columnNumber = columnMap[columnName];
  return columnNumber;
}

function aggregateSum(
  groupMap: GroupMap,
  leafData: readonly DataSourceRow[],
  columnMap: ColumnMap,
  aggregations: VuuAggregation[],
  targetData: readonly DataSourceRow[],
  groupIndices : number[],
): { [key: string]: number } {
  const sums: { [key: string]: number } = {};
  const aggColumn = getAggColumn(columnMap, aggregations);

  function sumRecursive(
    map: GroupMap | KeyList,
    leafData: readonly DataSourceRow[],
    aggColumn: number
  ) {
    if (Array.isArray(map)) {
      let sum = 0;
      for (const key of map) {
        sum += Number(leafData[key][aggColumn]);
      }
      return sum;
    } else {
      let sum = 0;
      for (const key in map) {
        sum += sumRecursive(map[key], leafData, aggColumn);
      }
      return sum;
    }
  }

  for (const key in groupMap) {
    const sum = Number(sumRecursive(groupMap[key], leafData, aggColumn));
    sums[key] = sum;
  }

  for (let index = 0; index < targetData.length; index++) {
    for (const key in sums) {
      if (targetData[index][groupIndices[0]] === key) {
        targetData[index][aggColumn] = sums[key];
      }
    }
  }

  return sums;
}

function aggregateAverage(
  groupMap: GroupMap,
  leafData: readonly DataSourceRow[],
  columnMap: ColumnMap,
  aggregations: VuuAggregation[],
  targetData: readonly DataSourceRow[],
  groupIndices : number[],
): { [key: string]: number } {
  const averages: { [key: string]: number } = {};
  const aggColumn = getAggColumn(columnMap, aggregations);

  let count = aggregateCount(groupMap, columnMap, aggregations, targetData, groupIndices);
  let sum = aggregateSum(
    groupMap,
    leafData,
    columnMap,
    aggregations,
    targetData,
    groupIndices
  );

  for (const key in count) {
    let average = 0;
    average = sum[key] / count[key];
    averages[key] = average;
  }

  for (let index = 0; index < targetData.length; index++) {
    for (const key in averages) {
      if (targetData[index][groupIndices[0]] === key) {
        targetData[index][aggColumn] = averages[key];
      }
    }
  }

  return averages;
}

function getLeafColumnData(
  map: GroupMap | KeyList,
  leafData: readonly DataSourceRow[],
  aggColumn: number,
) {
  let data = [];

  if (Array.isArray(map)) {
    for (const key of map) {
      data.push(leafData[key][aggColumn]);
    }
  } else {
    for (const key in map) {
      data.push(getLeafColumnData(map[key], leafData, aggColumn));
    }
  }

  return data;
}

function aggregateDistinct(
  groupMap: GroupMap,
  leafData: readonly DataSourceRow[],
  columnMap: ColumnMap,
  aggregations: VuuAggregation[],
  targetData: readonly DataSourceRow[],
  groupIndices : number[],
): { [key: string]: number } {
  const distincts: { [key: string]: number } = {};
  const aggColumn = getAggColumn(columnMap, aggregations);

  for (const key in groupMap) {
    const leafColumnData = getLeafColumnData(
      groupMap[key],
      leafData,
      aggColumn
    );
    const distinct: any = [...new Set(leafColumnData)];

    distincts[key] = distinct;
  }

  for (let index = 0; index < targetData.length; index++) {
    for (const key in distincts) {
      if (targetData[index][groupIndices[0]] === key) {
        targetData[index][aggColumn] = distincts[key];
      }
    }
  }

  return distincts;
}

function aggregateHigh(
  groupMap: GroupMap,
  leafData: readonly DataSourceRow[],
  columnMap: ColumnMap,
  aggregations: VuuAggregation[],
  targetData: readonly DataSourceRow[],
  groupIndices : number[],
): { [key: string]: number } {
  const highs: { [key: string]: number } = {};
  const aggColumn = getAggColumn(columnMap, aggregations);

  for (const key in groupMap) {
    const leafColumnData = getLeafColumnData(
      groupMap[key],
      leafData,
      aggColumn
    );
    const maxNumber = Math.max(...leafColumnData);

    highs[key] = maxNumber;
  }
  for (let index = 0; index < targetData.length; index++) {
    for (const key in highs) {
      if (targetData[index][groupIndices[0]] === key) {
        targetData[index][aggColumn] = highs[key];
      }
    }
  }

  return highs;
}

function aggregateLow(
  groupMap: GroupMap,
  leafData: readonly DataSourceRow[],
  columnMap: ColumnMap,
  aggregations: VuuAggregation[],
  targetData: readonly DataSourceRow[],
  groupIndices : number[],
): { [key: string]: number } {
  const mins: { [key: string]: number } = {};
  const aggColumn = getAggColumn(columnMap, aggregations);

  for (const key in groupMap) {
    const leafColumnData = getLeafColumnData(
      groupMap[key],
      leafData,
      aggColumn
    );
    const minNumber = Math.min(...leafColumnData);
    mins[key] = minNumber;
  }

  for (let index = 0; index < targetData.length; index++) {
    for (const key in mins) {
      if (targetData[index][groupIndices[0]] === key) {
        targetData[index][aggColumn] = mins[key];
      }
    }
  }

  return mins;
}
