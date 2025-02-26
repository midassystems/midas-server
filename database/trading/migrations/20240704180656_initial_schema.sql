-- Backtest Results --
CREATE TABLE IF NOT EXISTS Backtest (
    id SERIAL PRIMARY KEY,
    backtest_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS bt_Parameters (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    capital BIGINT NOT NULL, 
    schema VARCHAR(10),
    data_type VARCHAR(10) NOT NULL,
    "start" BIGINT NOT NULL,
    "end" BIGINT NOT NULL,
    tickers  TEXT[] NOT NULL,
    CONSTRAINT fk_bt_parameters
      FOREIGN KEY(backtest_id) 
        REFERENCES Backtest(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bt_StaticStats (
  id SERIAL PRIMARY KEY,
  backtest_id INTEGER NOT NULL,
  total_trades INTEGER NOT NULL,
  total_winning_trades INTEGER NOT NULL,
  total_losing_trades INTEGER NOT NULL,
  avg_profit BIGINT NOT NULL,
  avg_profit_percent BIGINT NOT NULL, 
  avg_gain BIGINT NOT NULL,
  avg_gain_percent BIGINT NOT NULL,
  avg_loss BIGINT NOT NULL,
  avg_loss_percent BIGINT NOT NULL,
  profitability_ratio BIGINT NOT NULL,
  profit_factor BIGINT NOT NULL,
  profit_and_loss_ratio BIGINT NOT NULL,
  total_fees BIGINT NOT NULL,  
  net_profit BIGINT NOT NULL, 
  beginning_equity BIGINT NOT NULL,  
  ending_equity BIGINT NOT NULL,  
  total_return BIGINT NOT NULL,
  annualized_return BIGINT NOT NULL,
  daily_standard_deviation_percentage BIGINT NOT NULL,
  annual_standard_deviation_percentage BIGINT NOT NULL,
  max_drawdown_percentage_period BIGINT NOT NULL,
  max_drawdown_percentage_daily BIGINT NOT NULL,
  sharpe_ratio BIGINT NOT NULL,
  sortino_ratio BIGINT NOT NULL,
  CONSTRAINT fk_bt_static_stats
    FOREIGN KEY(backtest_id) 
      REFERENCES Backtest(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bt_PeriodTimeseriesStats (
  id SERIAL PRIMARY KEY,
  backtest_id INTEGER NOT NULL,
  timestamp BIGINT,
  equity_value BIGINT DEFAULT 0,
  period_return BIGINT DEFAULT 0,
  cumulative_return BIGINT DEFAULT 0,
  percent_drawdown BIGINT DEFAULT 0,
  CONSTRAINT fk_bt_period_timeseries_stats
    FOREIGN KEY(backtest_id) 
      REFERENCES Backtest(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bt_DailyTimeseriesStats (
  id SERIAL PRIMARY KEY,
  backtest_id INTEGER NOT NULL,
  timestamp BIGINT,
  equity_value BIGINT DEFAULT 0,
  period_return BIGINT DEFAULT 0,
  cumulative_return BIGINT DEFAULT 0,
  percent_drawdown BIGINT DEFAULT 0,
  CONSTRAINT fk_bt_daily_timeseries_stats
    FOREIGN KEY(backtest_id) 
      REFERENCES Backtest(id)
      ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bt_Trade (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    trade_id INTEGER NOT NULL,
    signal_id INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    ticker VARCHAR(50) NOT NULL,
    quantity BIGINT NOT NULL,
    avg_price BIGINT NOT NULL,
    trade_value BIGINT NOT NULL,
    trade_cost BIGINT NOT NULL,
    action VARCHAR(10) NOT NULL,
    fees BIGINT NOT NULL,
    CONSTRAINT fk_bt_trade
      FOREIGN KEY(backtest_id) 
        REFERENCES Backtest(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bt_Signal (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    CONSTRAINT fk_bt_signal
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS bt_SignalInstructions (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    signal_id_fk INTEGER NOT NULL,
    ticker VARCHAR(100) NOT NULL,
    order_type VARCHAR(25) NOT NULL,
    action VARCHAR(10) NOT NULL,
    signal_id INTEGER NOT NULL,
    weight BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    limit_price VARCHAR(50) NOT NULL,
    aux_price VARCHAR(50) NOT NULL,
    CONSTRAINT fk_backtest_id
      FOREIGN KEY(backtest_id)
        REFERENCES Backtest(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_signal_id
      FOREIGN KEY(signal_id_fk)
        REFERENCES bt_Signal(id)
        ON DELETE CASCADE
);

-- Live Trading Results --
CREATE TABLE IF NOT EXISTS Live (
    id SERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS lv_Parameters (
    id SERIAL PRIMARY KEY,
    live_id INTEGER NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    capital BIGINT NOT NULL, 
    schema VARCHAR(10),
    data_type VARCHAR(10) NOT NULL,
    "start" BIGINT NOT NULL,
    "end" BIGINT NOT NULL,
    tickers  TEXT[] NOT NULL,
    CONSTRAINT fk_lv_parameters
      FOREIGN KEY(live_id) 
        REFERENCES Live(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lv_Trade (
    id SERIAL PRIMARY KEY,
    live_id INTEGER NOT NULL,
    trade_id INTEGER NOT NULL,
    signal_id INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    ticker VARCHAR(50) NOT NULL,
    quantity BIGINT NOT NULL,
    avg_price BIGINT NOT NULL,
    trade_value BIGINT NOT NULL,
    trade_cost BIGINT NOT NULL,
    action VARCHAR(10) NOT NULL,
    fees BIGINT NOT NULL,
    CONSTRAINT fk_lv_trade
      FOREIGN KEY(live_id) 
        REFERENCES Live(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lv_Signal (
    id SERIAL PRIMARY KEY,
    live_id INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    CONSTRAINT fk_lv_signal
        FOREIGN KEY(live_id) 
            REFERENCES Live(id)
            ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lv_SignalInstructions (
    id SERIAL PRIMARY KEY,
    live_id INTEGER NOT NULL,
    signal_id_fk INTEGER NOT NULL,
    ticker VARCHAR(100) NOT NULL,
    order_type VARCHAR(25) NOT NULL,
    action VARCHAR(10) NOT NULL,
    signal_id INTEGER NOT NULL,
    weight BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    limit_price VARCHAR(50) NOT NULL,
    aux_price VARCHAR(50) NOT NULL,
    CONSTRAINT fk_lv_id
      FOREIGN KEY(live_id)
        REFERENCES Live(id)
        ON DELETE CASCADE,
    CONSTRAINT fk_signal_id
      FOREIGN KEY(signal_id_fk)
        REFERENCES lv_Signal(id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS lv_AccountSummary (
  id SERIAL PRIMARY KEY,
  live_id INTEGER NOT NULL,
  currency VARCHAR(4) NOT NULL,
  start_timestamp BIGINT,
  start_buying_power BIGINT NOT NULL,
  start_excess_liquidity BIGINT NOT NULL,
  start_full_available_funds BIGINT NOT NULL,
  start_full_init_margin_req BIGINT NOT NULL,
  start_full_maint_margin_req BIGINT NOT NULL,
  start_futures_pnl BIGINT NOT NULL,
  start_net_liquidation BIGINT NOT NULL,
  start_total_cash_balance BIGINT NOT NULL,
  start_unrealized_pnl BIGINT NOT NULL,
  end_timestamp BIGINT,
  end_buying_power BIGINT NOT NULL,
  end_excess_liquidity BIGINT NOT NULL,
  end_full_available_funds BIGINT NOT NULL,
  end_full_init_margin_req BIGINT NOT NULL,
  end_full_maint_margin_req BIGINT NOT NULL,
  end_futures_pnl BIGINT NOT NULL,
  end_net_liquidation BIGINT NOT NULL,
  end_total_cash_balance BIGINT NOT NULL,
  end_unrealized_pnl BIGINT NOT NULL,
  CONSTRAINT fk_lv_account_summary
    FOREIGN KEY(live_id) 
      REFERENCES Live(id)
      ON DELETE CASCADE
);

-- other -- 
-- CREATE TABLE lv_Signal (
--     id SERIAL PRIMARY KEY,
--     live_session_id INTEGER NOT NULL,
--     timestamp BIGINT,
--     CONSTRAINT fk_live_session_signal
--         FOREIGN KEY(live_session_id) 
--             REFERENCES LiveSession(id)
--             ON DELETE CASCADE
-- );
--
-- CREATE TABLE livesession_TradeInstruction (
--     id SERIAL PRIMARY KEY,
--     signal_id INTEGER NOT NULL,
--     ticker VARCHAR(100) NOT NULL,
--     action VARCHAR(10) NOT NULL,
--     trade_id INTEGER NOT NULL,
--     leg_id INTEGER NOT NULL,
--     weight FLOAT NOT NULL,
--     CONSTRAINT fk_signal_trade_instruction
--         FOREIGN KEY(signal_id) 
--             REFERENCES livesession_Signal(id)
--             ON DELETE CASCADE
-- );
--
-- CREATE TABLE livesession_Trade (
--     id SERIAL PRIMARY KEY,
--     live_session_id INTEGER NOT NULL,
--     timestamp BIGINT,
--     ticker VARCHAR(50) NOT NULL,
--     quantity DECIMAL(10, 4) NOT NULL,
--     avg_price DECIMAL(10, 4) DEFAULT 0.0 NOT NULL,
--     trade_value DECIMAL(15, 4) DEFAULT 0.0 NOT NULL,
--     action VARCHAR(10) NOT NULL,
--     fees DECIMAL(10, 4) NOT NULL,
--     CONSTRAINT fk_live_session_trade
--         FOREIGN KEY(live_session_id) 
--             REFERENCES LiveSession(id)
--             ON DELETE CASCADE
-- );


-- Live Session -- 
-- CREATE TABLE Session (
--     session_id BIGINT PRIMARY KEY UNIQUE
-- );


-- CREATE TABLE Position (
--     id SERIAL PRIMARY KEY,
--     session_id BIGINT UNIQUE NOT NULL,
--     data JSONB NOT NULL,
--     CONSTRAINT fk_session_position
--         FOREIGN KEY(session_id) 
--             REFERENCES Session(session_id)
--             ON DELETE CASCADE
-- );
-- CREATE TABLE Account (
--     id SERIAL PRIMARY KEY,
--     session_id BIGINT UNIQUE NOT NULL,
--     data JSONB NOT NULL,
--     CONSTRAINT fk_session_account
--         FOREIGN KEY(session_id) 
--             REFERENCES Session(session_id)
--             ON DELETE CASCADE
-- );

-- CREATE TABLE Order (
--     id SERIAL PRIMARY KEY,
--     session_id BIGINT UNIQUE NOT NULL,
--     data JSONB NOT NULL,
--     CONSTRAINT fk_session_order
--         FOREIGN KEY(session_id) 
--             REFERENCES Session(session_id)
--             ON DELETE CASCADE
-- );
-- CREATE TABLE Risk (
--     id SERIAL PRIMARY KEY,
--     session_id BIGINT UNIQUE NOT NULL,
--     data JSONB NOT NULL,
--     CONSTRAINT fk_session_risk
--         FOREIGN KEY(session_id) 
--             REFERENCES Session(session_id)
--             ON DELETE CASCADE
-- );
-- CREATE TABLE MarketData (
--     id SERIAL PRIMARY KEY,
--     session_id BIGINT UNIQUE NOT NULL,
--     data JSONB NOT NULL,
--     CONSTRAINT fk_session_marketdata
--         FOREIGN KEY(session_id) 
--             REFERENCES Session(session_id)
--             ON DELETE CASCADE
-- );




--  
-- -- Regression Analysis -- 
-- CREATE TABLE RegressionAnalysis (
--     id SERIAL PRIMARY KEY,
--     backtest_id INTEGER NOT NULL,
--     risk_free_rate DECIMAL(5, 4) NOT NULL,
--     r_squared DECIMAL(10, 7) NOT NULL CHECK (r_squared >= 0.0 AND r_squared <= 1.0),
--     adjusted_r_squared DECIMAL(10, 7) NOT NULL CHECK (adjusted_r_squared >= 0.0 AND adjusted_r_squared <= 1.0),
--     RMSE DECIMAL(10, 7) NOT NULL,
--     MAE DECIMAL(10, 7) NOT NULL,
--     f_statistic DECIMAL(10, 7) NOT NULL,
--     f_statistic_p_value DECIMAL(10, 7) NOT NULL,
--     durbin_watson DECIMAL(10, 7) NOT NULL,
--     jarque_bera DECIMAL(10, 7) NOT NULL,
--     jarque_bera_p_value DECIMAL(10, 7) NOT NULL,
--     condition_number DECIMAL(10, 7) NOT NULL,
--     vif JSONB DEFAULT '{}'::jsonb NOT NULL,
--     alpha DECIMAL(15, 7) NOT NULL,
--     p_value_alpha DECIMAL(15, 7) NOT NULL,
--     beta JSONB DEFAULT '{}'::jsonb NOT NULL,
--     p_value_beta JSONB DEFAULT '{}'::jsonb NOT NULL,
--     total_contribution DECIMAL(10, 7) NOT NULL,
--     systematic_contribution DECIMAL(10, 7) NOT NULL,
--     idiosyncratic_contribution DECIMAL(10, 7) NOT NULL,
--     total_volatility DECIMAL(10, 7) NOT NULL,
--     systematic_volatility DECIMAL(10, 7) NOT NULL,
--     idiosyncratic_volatility DECIMAL(10, 7) NOT NULL,
--     residuals JSONB DEFAULT '[]'::jsonb,
--     CONSTRAINT fk_backtest
--         FOREIGN KEY(backtest_id) 
--             REFERENCES Backtest(id)
--             ON DELETE CASCADE
-- );
--
-- CREATE TABLE regressionanalysis_TimeSeriesData (
--     id SERIAL PRIMARY KEY,
--     regression_analysis_id INTEGER NOT NULL,
--     timestamp BIGINT,
--     daily_benchmark_return DECIMAL(15, 6) NOT NULL,
--     CONSTRAINT fk_regression_analysis
--         FOREIGN KEY(regression_analysis_id)
--             REFERENCES RegressionAnalysis(id)
--             ON DELETE CASCADE
-- );
