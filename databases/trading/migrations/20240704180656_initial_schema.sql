-- Add migration script herei

-- Backtest Results --
CREATE TABLE Backtest (
    id SERIAL PRIMARY KEY,
    backtest_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);


CREATE TABLE Parameters (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    strategy_name VARCHAR(255) NOT NULL,
    capital FLOAT NOT NULL,
    data_type VARCHAR(10) NOT NULL,
    train_start BIGINT NOT NULL,
    train_end BIGINT NOT NULL,
    test_start BIGINT NOT NULL,
    test_end BIGINT NOT NULL,
    tickers JSONB NOT NULL,
    CONSTRAINT fk_backtest_parameters
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);


CREATE TABLE backtest_Trade (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    trade_id INTEGER NOT NULL,
    leg_id INTEGER NOT NULL,
    timestamp BIGINT NOT NULL,
    ticker VARCHAR(50) NOT NULL,
    quantity DECIMAL(10, 4) NOT NULL,
    avg_price DECIMAL(10, 4) NOT NULL,
    trade_value DECIMAL(15, 4) NOT NULL,
    action VARCHAR(10) NOT NULL,
    fees DECIMAL(10, 4) NOT NULL,
    CONSTRAINT fk_backtest_trade
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);

CREATE TABLE backtest_Signal (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    timestamp BIGINT,
    trade_instructions JSONB NOT NULL,
    CONSTRAINT fk_backtest_signal
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);

-- CREATE TABLE backtest_TradeInstruction (
--     id SERIAL PRIMARY KEY,
--     signal_id INTEGER NOT NULL,
--     ticker VARCHAR(100) NOT NULL,
--     action VARCHAR(10) NOT NULL,
--     trade_id INTEGER NOT NULL,
--     leg_id INTEGER NOT NULL,
--     weight FLOAT NOT NULL,
--     CONSTRAINT fk_signal_trade_instruction
--         FOREIGN KEY(signal_id) 
--             REFERENCES backtest_Signal(id)
--             ON DELETE CASCADE
-- );

CREATE TABLE backtest_StaticStats (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    net_profit FLOAT NOT NULL,
    total_fees FLOAT NOT NULL,
    ending_equity FLOAT NOT NULL,
    avg_trade_profit FLOAT NOT NULL,
    total_return FLOAT NOT NULL,
    annual_standard_deviation_percentage FLOAT NOT NULL,
    max_drawdown_percentage FLOAT NOT NULL,
    avg_win_percentage FLOAT NOT NULL,
    avg_loss_percentage FLOAT NOT NULL,
    percent_profitable FLOAT NOT NULL,
    total_trades INTEGER NOT NULL,
    number_winning_trades INTEGER NOT NULL,
    number_losing_trades INTEGER NOT NULL,
    profit_and_loss_ratio FLOAT NOT NULL,
    profit_factor FLOAT NOT NULL,
    sortino_ratio FLOAT NOT NULL,
    sharpe_ratio FLOAT NOT NULL,
    CONSTRAINT fk_backtest_static_stats
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);

CREATE TABLE backtest_PeriodTimeseriesStats (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    timestamp BIGINT,
    equity_value DECIMAL(15, 2) DEFAULT 0.0,
    period_return DECIMAL(15, 6) DEFAULT 0.0,
    cumulative_return DECIMAL(15, 6) DEFAULT 0.0,
    percent_drawdown DECIMAL(15, 6) DEFAULT 0.0,
    CONSTRAINT fk_backtest_period_timeseries_stats
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);

CREATE TABLE backtest_DailyTimeseriesStats (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    timestamp BIGINT,
    equity_value DECIMAL(15, 2) DEFAULT 0.0,
    period_return DECIMAL(15, 6) DEFAULT 0.0,
    cumulative_return DECIMAL(15, 6) DEFAULT 0.0,
    percent_drawdown DECIMAL(15, 6) DEFAULT 0.0,
    CONSTRAINT fk_backtest_daily_timeseries_stats
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);

-- Regression Analysis -- 
CREATE TABLE RegressionAnalysis (
    id SERIAL PRIMARY KEY,
    backtest_id INTEGER NOT NULL,
    risk_free_rate DECIMAL(5, 4) NOT NULL,
    r_squared DECIMAL(10, 7) NOT NULL CHECK (r_squared >= 0.0 AND r_squared <= 1.0),
    adjusted_r_squared DECIMAL(10, 7) NOT NULL CHECK (adjusted_r_squared >= 0.0 AND adjusted_r_squared <= 1.0),
    RMSE DECIMAL(10, 7) NOT NULL,
    MAE DECIMAL(10, 7) NOT NULL,
    f_statistic DECIMAL(10, 7) NOT NULL,
    f_statistic_p_value DECIMAL(10, 7) NOT NULL,
    durbin_watson DECIMAL(10, 7) NOT NULL,
    jarque_bera DECIMAL(10, 7) NOT NULL,
    jarque_bera_p_value DECIMAL(10, 7) NOT NULL,
    condition_number DECIMAL(10, 7) NOT NULL,
    vif JSONB DEFAULT '{}'::jsonb NOT NULL,
    alpha DECIMAL(15, 7) NOT NULL,
    p_value_alpha DECIMAL(15, 7) NOT NULL,
    beta JSONB DEFAULT '{}'::jsonb NOT NULL,
    p_value_beta JSONB DEFAULT '{}'::jsonb NOT NULL,
    total_contribution DECIMAL(10, 7) NOT NULL,
    systematic_contribution DECIMAL(10, 7) NOT NULL,
    idiosyncratic_contribution DECIMAL(10, 7) NOT NULL,
    total_volatility DECIMAL(10, 7) NOT NULL,
    systematic_volatility DECIMAL(10, 7) NOT NULL,
    idiosyncratic_volatility DECIMAL(10, 7) NOT NULL,
    residuals JSONB DEFAULT '[]'::jsonb,
    CONSTRAINT fk_backtest
        FOREIGN KEY(backtest_id) 
            REFERENCES Backtest(id)
            ON DELETE CASCADE
);

CREATE TABLE regressionanalysis_TimeSeriesData (
    id SERIAL PRIMARY KEY,
    regression_analysis_id INTEGER NOT NULL,
    timestamp BIGINT,
    daily_benchmark_return DECIMAL(15, 6) NOT NULL,
    CONSTRAINT fk_regression_analysis
        FOREIGN KEY(regression_analysis_id)
            REFERENCES RegressionAnalysis(id)
            ON DELETE CASCADE
);

-- Trading Results --
CREATE TABLE LiveSession (
    id SERIAL PRIMARY KEY,
    strategy_name VARCHAR(255) NOT NULL,
    tickers JSONB DEFAULT '[]'::jsonb NOT NULL,
    benchmark JSONB DEFAULT '[]'::jsonb NOT NULL,
    data_type VARCHAR(10) NOT NULL,
    train_start BIGINT,
    train_end BIGINT,
    test_start BIGINT,
    test_end BIGINT,
    capital FLOAT,
    strategy_allocation FLOAT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL
);


CREATE TABLE livesession_Signal (
    id SERIAL PRIMARY KEY,
    live_session_id INTEGER NOT NULL,
    timestamp BIGINT,
    CONSTRAINT fk_live_session_signal
        FOREIGN KEY(live_session_id) 
            REFERENCES LiveSession(id)
            ON DELETE CASCADE
);

CREATE TABLE livesession_TradeInstruction (
    id SERIAL PRIMARY KEY,
    signal_id INTEGER NOT NULL,
    ticker VARCHAR(100) NOT NULL,
    action VARCHAR(10) NOT NULL,
    trade_id INTEGER NOT NULL,
    leg_id INTEGER NOT NULL,
    weight FLOAT NOT NULL,
    CONSTRAINT fk_signal_trade_instruction
        FOREIGN KEY(signal_id) 
            REFERENCES livesession_Signal(id)
            ON DELETE CASCADE
);

CREATE TABLE livesession_Trade (
    id SERIAL PRIMARY KEY,
    live_session_id INTEGER NOT NULL,
    timestamp BIGINT,
    ticker VARCHAR(50) NOT NULL,
    quantity DECIMAL(10, 4) NOT NULL,
    avg_price DECIMAL(10, 4) DEFAULT 0.0 NOT NULL,
    trade_value DECIMAL(15, 4) DEFAULT 0.0 NOT NULL,
    action VARCHAR(10) NOT NULL,
    fees DECIMAL(10, 4) NOT NULL,
    CONSTRAINT fk_live_session_trade
        FOREIGN KEY(live_session_id) 
            REFERENCES LiveSession(id)
            ON DELETE CASCADE
);
CREATE TABLE livesession_AccountSummary (
    id SERIAL PRIMARY KEY,
    live_session_id INTEGER NOT NULL,
    currency VARCHAR(4) NOT NULL,
    start_timestamp BIGINT,
    start_buying_power DECIMAL(15, 4) NOT NULL,
    start_excess_liquidity DECIMAL(15, 4) NOT NULL,
    start_full_available_funds DECIMAL(15, 4) NOT NULL,
    start_full_init_margin_req DECIMAL(15, 4) NOT NULL,
    start_full_maint_margin_req DECIMAL(15, 4) NOT NULL,
    start_futures_pnl DECIMAL(15, 4) NOT NULL,
    start_net_liquidation DECIMAL(15, 4) NOT NULL,
    start_total_cash_balance DECIMAL(15, 4) NOT NULL,
    start_unrealized_pnl DECIMAL(15, 4) NOT NULL,
    end_timestamp BIGINT,
    end_buying_power DECIMAL(15, 4) NOT NULL,
    end_excess_liquidity DECIMAL(15, 4) NOT NULL,
    end_full_available_funds DECIMAL(15, 4) NOT NULL,
    end_full_init_margin_req DECIMAL(15, 4) NOT NULL,
    end_full_maint_margin_req DECIMAL(15, 4) NOT NULL,
    end_futures_pnl DECIMAL(15, 4) NOT NULL,
    end_net_liquidation DECIMAL(15, 4) NOT NULL,
    end_total_cash_balance DECIMAL(15, 4) NOT NULL,
    end_unrealized_pnl DECIMAL(16, 4) NOT NULL,
    CONSTRAINT fk_live_session_account_summary
        FOREIGN KEY(live_session_id) 
            REFERENCES LiveSession(id)
            ON DELETE CASCADE
);
 
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



