# ETL Forecasting Test Plan

This document outlines the comprehensive testing strategy for the ETL Forecasting project, including test environment setup, data refresh procedures, validation steps, and promotion criteria for moving to production.

## Test Environment Overview

The test environment (`FCD_090_DB_DEV`) serves as the staging area for validating all ETL processes before production deployment. This environment mirrors production data structures while providing a safe space for testing and validation.

### Test Environment Databases
- **Development Schema**: `FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA`
- **Source Tables**: Mirror production with test data
- **Output Tables**: Same structure as production for validation

## Test Data Refresh Strategy

### 1. Data Refresh Schedule
- **Frequency**: Weekly or as needed for testing cycles
- **Timing**: During off-peak hours to minimize impact
- **Source**: Production data subset or synthetic test data

### 2. Data Refresh Procedures

#### Step 1: Pre-Refresh Validation
```sql
-- Verify current test data state
SELECT COUNT(*) FROM FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.INVOICE;
SELECT COUNT(*) FROM FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_APP;
SELECT COUNT(*) FROM FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_REQ;
SELECT COUNT(*) FROM FCD_090_DB.UNIFIER.UNIFIER_COSTSHEET_SUBSTAGE_FUND;
SELECT COUNT(*) FROM FCD_090_DB.HCFCD_PROJ_COSTSCHED.P6_SCHEDULE;
```

#### Step 2: Backup Current Test Data
```sql
-- Create backup tables before refresh
CREATE TABLE INVOICE_BACKUP AS SELECT * FROM FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.INVOICE;
CREATE TABLE PAY_APP_BACKUP AS SELECT * FROM FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_APP;
CREATE TABLE PAY_REQ_BACKUP AS SELECT * FROM FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.PAY_REQ;
```

#### Step 3: Data Refresh Execution
```sql
-- Truncate and reload test tables with fresh data
TRUNCATE TABLE FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.INVOICE;
INSERT INTO FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.INVOICE 
SELECT * FROM PRODUCTION.PROJECT_CASHFLOW_DATA.INVOICE 
WHERE INVOICE_DATE >= DATEADD(YEAR, -2, CURRENT_DATE());
```

#### Step 4: Post-Refresh Validation
```sql
-- Verify data integrity after refresh
SELECT 
    COUNT(*) as record_count,
    MIN(INVOICE_DATE) as earliest_date,
    MAX(INVOICE_DATE) as latest_date,
    COUNT(DISTINCT PROJECTID) as unique_projects
FROM FCD_090_DB_DEV.PROJECT_CASHFLOW_DATA.INVOICE;
```

## Testing Phases and Milestones

### Phase 1: Unit Testing
**Objective**: Validate individual script functionality

#### Milestone 1.1: ACTUALVALUE.txt Testing
- [ ] **Data Loading**: Verify all three invoice sources load correctly
- [ ] **Data Integration**: Confirm joins with budget and schedule data
- [ ] **Deduplication Logic**: Test removal of offsetting transactions
- [ ] **Cumulative Calculation**: Validate cumulative cost calculations
- [ ] **Output Validation**: Verify ACTUALVALUE table creation

**Acceptance Criteria**:
```python
# Test: Cumulative costs should be monotonically increasing
def test_cumulative_increase():
    df = session.table("ACTUALVALUE")
    # Group by project/stage/substage and verify cumulative increases
    assert all(cumulative_costs_are_increasing)
```

#### Milestone 1.2: PLANNEDVALUE.txt Testing
- [ ] **Normalized Curve Loading**: Verify curve data loads correctly
- [ ] **Schedule Integration**: Test mapping to project timelines
- [ ] **Daily Distribution**: Validate daily planned value generation
- [ ] **Curve Application**: Confirm normalized percentages applied correctly
- [ ] **Output Validation**: Verify PLANNEDVALUE table creation

**Acceptance Criteria**:
```python
# Test: Planned values should sum to budget
def test_planned_vs_budget():
    planned_sum = sum(planned_values_by_project)
    budget_total = project_budget
    assert abs(planned_sum - budget_total) < 0.01
```

#### Milestone 1.3: FORECASTVALUE.txt Testing
- [ ] **Dependency Check**: Verify ACTUALVALUE and PLANNEDVALUE exist
- [ ] **Remaining Budget**: Test calculation of remaining amounts
- [ ] **Future Filtering**: Confirm only future dates included
- [ ] **Proportional Distribution**: Validate forecast distribution logic
- [ ] **Cumulative Forecast**: Test forecast continuation from actuals
- [ ] **Output Validation**: Verify FORECASTVALUE table creation

**Acceptance Criteria**:
```python
# Test: Forecast should start from last actual cumulative
def test_forecast_continuity():
    last_actual = get_last_actual_cumulative()
    first_forecast = get_first_forecast_cumulative()
    assert first_forecast >= last_actual
```

#### Milestone 1.4: COMBINEDCURVES.txt Testing
- [ ] **Data Standardization**: Test column name normalization
- [ ] **Type Identification**: Verify TYPE column assignment
- [ ] **Data Concatenation**: Confirm all datasets combined
- [ ] **Data Type Consistency**: Validate consistent data types
- [ ] **Output Validation**: Verify COMBINED_CURVES table creation

### Phase 2: Integration Testing
**Objective**: Validate end-to-end process flow

#### Milestone 2.1: Data Flow Testing
- [ ] **Sequential Execution**: Test scripts in correct order
- [ ] **Data Dependencies**: Verify downstream dependencies
- [ ] **Error Handling**: Test behavior with missing/invalid data
- [ ] **Performance**: Validate execution times within acceptable limits

#### Milestone 2.2: Data Quality Testing
- [ ] **Completeness**: All expected projects/stages present
- [ ] **Accuracy**: Sample validation against source systems
- [ ] **Consistency**: Cross-table data consistency checks
- [ ] **Timeliness**: Data freshness validation

**Data Quality Checks**:
```sql
-- Completeness Check
SELECT 
    'ACTUAL' as source,
    COUNT(DISTINCT PROJECT_NUMBER) as project_count
FROM ACTUALVALUE
UNION ALL
SELECT 
    'PLANNED' as source,
    COUNT(DISTINCT PROJECT_NUMBER) as project_count
FROM PLANNEDVALUE
UNION ALL
SELECT 
    'FORECAST' as source,
    COUNT(DISTINCT PROJECT_NUMBER) as project_count
FROM FORECASTVALUE;

-- Consistency Check
SELECT 
    a.PROJECT_NUMBER,
    a.max_actual_date,
    f.min_forecast_date,
    CASE WHEN f.min_forecast_date > a.max_actual_date THEN 'PASS' ELSE 'FAIL' END as continuity_check
FROM (
    SELECT PROJECT_NUMBER, MAX(INVOICE_DATE) as max_actual_date
    FROM ACTUALVALUE GROUP BY PROJECT_NUMBER
) a
JOIN (
    SELECT PROJECT_NUMBER, MIN(FORECAST_DATE) as min_forecast_date
    FROM FORECASTVALUE GROUP BY PROJECT_NUMBER
) f ON a.PROJECT_NUMBER = f.PROJECT_NUMBER;
```

### Phase 3: User Acceptance Testing
**Objective**: Validate business requirements and user workflows

#### Milestone 3.1: Business Logic Validation
- [ ] **Spending Curves**: Verify curves match expected patterns
- [ ] **Budget Alignment**: Confirm forecasts align with budgets
- [ ] **Timeline Accuracy**: Validate against project schedules
- [ ] **Stakeholder Review**: Business user validation

#### Milestone 3.2: Reporting Integration
- [ ] **Power BI Connection**: Test COMBINED_CURVES table access
- [ ] **Visualization**: Validate charts and dashboards
- [ ] **Performance**: Confirm acceptable query response times
- [ ] **User Training**: Documentation and training completion

### Phase 4: Performance and Scale Testing
**Objective**: Validate system performance under expected loads

#### Milestone 4.1: Volume Testing
- [ ] **Large Dataset**: Test with full production data volume
- [ ] **Memory Usage**: Monitor memory consumption
- [ ] **Execution Time**: Validate processing times
- [ ] **Resource Utilization**: Monitor Snowflake compute usage

#### Milestone 4.2: Concurrent User Testing
- [ ] **Multi-User Access**: Test concurrent COMBINED_CURVES access
- [ ] **Lock Contention**: Monitor for table locking issues
- [ ] **Query Performance**: Validate under concurrent load

## Production Promotion Criteria

### Technical Requirements
- [ ] All unit tests passing (100% success rate)
- [ ] Integration tests completed successfully
- [ ] Performance benchmarks met
- [ ] Data quality validations passed
- [ ] Error handling tested and documented
- [ ] Rollback procedures tested

### Business Requirements
- [ ] User acceptance testing completed
- [ ] Business stakeholder sign-off
- [ ] Documentation completed and approved
- [ ] Training materials prepared
- [ ] Support procedures documented

### Operational Requirements
- [ ] Production deployment plan approved
- [ ] Monitoring and alerting configured
- [ ] Backup and recovery procedures tested
- [ ] Change management process followed
- [ ] Go-live support plan established

## Promotion Process

### Step 1: Pre-Deployment Checklist
```bash
# Verify all promotion criteria met
- Technical requirements: ✓
- Business requirements: ✓
- Operational requirements: ✓
- Stakeholder approvals: ✓
```

### Step 2: Production Deployment
1. **Database Schema Updates**: Apply any schema changes
2. **Code Deployment**: Deploy validated scripts to production
3. **Initial Data Load**: Execute full ETL process
4. **Validation**: Run production validation tests
5. **Go-Live**: Enable production access

### Step 3: Post-Deployment Validation
- [ ] **Immediate Validation**: Verify tables created successfully
- [ ] **Data Quality Check**: Run production data quality tests
- [ ] **Performance Monitoring**: Monitor initial performance metrics
- [ ] **User Acceptance**: Confirm user access and functionality

### Step 4: Ongoing Monitoring
- Daily data refresh monitoring
- Performance metric tracking
- User feedback collection
- Issue escalation procedures

## Risk Mitigation

### High-Risk Scenarios
1. **Data Corruption**: Backup and recovery procedures
2. **Performance Degradation**: Rollback to previous version
3. **Business Logic Errors**: Validation checkpoints
4. **User Access Issues**: Alternative access methods

### Rollback Procedures
1. **Immediate**: Stop current process, revert to backup tables
2. **Short-term**: Deploy previous validated version
3. **Long-term**: Full system rollback with data restoration

## Test Execution Schedule

| Phase | Duration | Dependencies | Deliverables |
|-------|----------|--------------|--------------|
| Unit Testing | 2 weeks | Test data refresh | Test results, bug fixes |
| Integration Testing | 1 week | Unit tests complete | Integration validation |
| UAT | 1 week | Integration complete | Business sign-off |
| Performance Testing | 3 days | UAT complete | Performance report |
| Production Prep | 1 week | All testing complete | Deployment plan |

## Success Metrics

- **Data Accuracy**: 99.9% data quality score
- **Performance**: ETL completion within 2-hour window
- **Reliability**: 99.5% successful execution rate
- **User Satisfaction**: 95% positive feedback score

## Contact Information

- **Test Lead**: [Name, Email]
- **Business Owner**: [Name, Email]
- **Technical Lead**: [Name, Email]
- **DBA Support**: [Name, Email]
