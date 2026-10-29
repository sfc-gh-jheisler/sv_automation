# SemanticNet Demo: Sample Batch Descriptions

Copy and paste any of the blocks below into the Free-Form input in SemanticNet.

---

## Batch 1: DIM_CLIENT + DIM_PORTFOLIO + FACT_POSITION_DAILY_ABOR

This covers the core client-portfolio-positions star schema. Paste the entire block below.

```
In DIM_CLIENT: CLIENTID is the unique identifier assigned to each institutional client relationship. CLIENTNAME is the legal entity name of the client as registered in the onboarding system. CLIENTTYPE classifies the client as institutional, pension fund, endowment, sovereign wealth, or family office. REGION is the geographic region the client is domiciled in for regulatory and reporting purposes. AUM_WITH_SAM is the total assets under management the client has placed with our firm in USD. RELATIONSHIPSTARTDATE is the date the client relationship was formally established. PRIMARYCONTACT is the name of the lead relationship manager assigned to this client. ACCOUNTSTATUS indicates whether the client account is active, dormant, or terminated.

In DIM_PORTFOLIO: PORTFOLIOID is the unique identifier for each managed portfolio. PORTFOLIOCODE is the short alphanumeric code used to reference the portfolio in trading and reporting systems. PORTFOLIONAME is the descriptive name of the portfolio. STRATEGY is the investment strategy the portfolio follows such as core equity, fixed income, multi-asset, or alternatives. BASECURRENCY is the ISO currency code used for the portfolio's base reporting currency. INCEPTIONDATE is the date the portfolio was created and first funded. BENCHMARKID is the foreign key linking to the benchmark the portfolio is measured against.

In FACT_POSITION_DAILY_ABOR: HOLDINGDATE is the business date for which this position snapshot is recorded. PORTFOLIOID is the foreign key linking to the portfolio that holds this position. SECURITYID is the foreign key linking to the security held. QUANTITY is the number of shares or units held in the position. MARKETVALUE_LOCAL is the current market value of the position in the security's local trading currency. MARKETVALUE_BASE is the current market value converted to the portfolio's base currency. COSTBASIS_LOCAL is the original cost basis in local currency. COSTBASIS_BASE is the original cost basis converted to base currency. ACCRUEDINTEREST_LOCAL is accrued but unpaid interest on fixed income positions in local currency. PORTFOLIOWEIGHT is the position's weight as a percentage of total portfolio market value.
```

---

## Batch 2: Just DIM_CLIENT (shorter, for quick demo)

```
In DIM_CLIENT: CLIENTID is the unique identifier for each client. CLIENTNAME is the registered legal name. CLIENTTYPE classifies the client as institutional, pension, endowment, sovereign wealth, or family office. REGION is where the client is domiciled. AUM_WITH_SAM is total assets the client has placed with us in USD. RELATIONSHIPSTARTDATE is when we started the relationship. PRIMARYCONTACT is the assigned relationship manager. ACCOUNTSTATUS is whether the account is active, dormant, or terminated.
```
