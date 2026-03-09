# technical_analysis.py
import sys
import os
os.environ['CRYPTOGRAPHY_OPENSSL_NO_LEGACY'] = '1'
import pandas as pd
# import talib -> REMOVED
import numpy as np
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional

# --- PATH SETUP ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- IMPORTS ---
try:
    # This import is primarily for the CLI testing functionality below.
    from src.services.market_resolver import MarketResolver
    _MarketResolver_imported = True
except ImportError:
    _MarketResolver_imported = False
    class MarketResolver: pass
    logging.warning("MarketResolver not found. CLI testing requiring it may fail.")

try:
    from utils.logger import get_logger
    logger = get_logger("TechnicalAnalysis")
except ImportError:
    logging.warning("get_logger utility not found. Using basic logging for TechnicalAnalysis.")
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("TechnicalAnalysis_Fallback")

class TechnicalAnalyzer:
    """
    Analyzes OHLCV data to generate a wide range of technical indicators using pandas and numpy,
    and provides a structured summary of the technical outlook.
    """
    def __init__(self, df_ohlcv: pd.DataFrame):
        """
        Initializes the analyzer with the OHLCV DataFrame.

        Args:
            df_ohlcv (pd.DataFrame): DataFrame with 'open', 'high', 'low', 'close', 'volume' columns.
                                     A datetime index is expected.
        """
        if not isinstance(df_ohlcv, pd.DataFrame):
            logger.error("TechnicalAnalyzer initialized with non-DataFrame input.")
            self.df_ohlcv_original = pd.DataFrame()
            self.df_with_indicators = pd.DataFrame()
            return

        self.df_ohlcv_original = df_ohlcv.copy()

        if df_ohlcv.empty:
            logger.warning("TechnicalAnalyzer initialized with empty DataFrame.")
            self.df_with_indicators = pd.DataFrame()
            return

        df_processed = self._prepare_dataframe(df_ohlcv)
        self.df_with_indicators = df_processed
        logger.info(f"TechnicalAnalyzer initialized with {len(df_processed)} data points.")

    def _prepare_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardizes and cleans the input DataFrame."""
        df_processed = df.copy()
        rename_map = {
            "Timestamp": "timestamp", "Date": "timestamp", "date": "timestamp",
            "Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"
        }
        # Standardize column names to lowercase
        df_processed.columns = [col.lower() for col in df_processed.columns]
        actual_rename_map = {k.lower(): v for k, v in rename_map.items() if k.lower() in df_processed.columns}
        df_processed.rename(columns=actual_rename_map, inplace=True)


        if 'timestamp' in df_processed.columns:
            df_processed['timestamp'] = pd.to_datetime(df_processed['timestamp'])
            if not pd.api.types.is_datetime64_any_dtype(df_processed.index):
                df_processed.set_index('timestamp', inplace=True)
        elif not pd.api.types.is_datetime64_any_dtype(df_processed.index):
             logger.warning("DataFrame has no 'timestamp' column and index is not datetime. Time-based ops may fail.")

        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col in df_processed.columns:
                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
            else:
                logger.warning(f"Required column '{col}' not found. Creating empty column.")
                df_processed[col] = np.nan

        initial_rows = len(df_processed)
        df_processed.dropna(subset=required_cols, inplace=True)
        if len(df_processed) < initial_rows:
            logger.info(f"Dropped {initial_rows - len(df_processed)} rows with NaN in essential OHLCV columns.")

        return df_processed

    def _add_golden_death_crosses(self) -> None:
        """Calculates and adds Golden Cross and Death Cross signals to the DataFrame."""
        df = self.df_with_indicators
        if 'SMA_50' in df.columns and 'SMA_200' in df.columns:
            # Golden Cross: SMA50 crosses above SMA200
            condition_golden_curr = (df['SMA_50'] > df['SMA_200'])
            condition_golden_prev = (df['SMA_50'].shift(1) <= df['SMA_200'].shift(1))
            df['GOLDEN_X'] = (condition_golden_curr & condition_golden_prev).astype(int)

            # Death Cross: SMA50 crosses below SMA200
            condition_death_curr = (df['SMA_50'] < df['SMA_200'])
            condition_death_prev = (df['SMA_50'].shift(1) >= df['SMA_200'].shift(1))
            df['DEATH_X'] = (condition_death_curr & condition_death_prev).astype(int)
        else:
            logger.warning("SMA_50 or SMA_200 not available. Cannot calculate Golden/Death crosses.")
            df['GOLDEN_X'] = 0
            df['DEATH_X'] = 0

    def _calculate_sar(self, high: pd.Series, low: pd.Series, acceleration: float = 0.02, maximum: float = 0.2) -> pd.Series:
        """Calculates Parabolic SAR iteratively."""
        length = len(high)
        # Initialize arrays and starting values
        psar = low.copy()
        bull = True
        af = acceleration
        ep = high[0]

        for i in range(2, length):
            # Calculate PSAR for the current period
            if bull:
                psar[i] = psar[i - 1] + af * (ep - psar[i - 1])
            else:
                psar[i] = psar[i - 1] - af * (ep - psar[i - 1])

            # Check for reversal
            reverse = False
            if bull:
                if low[i] < psar[i]:
                    bull = False
                    reverse = True
                    psar[i] = ep
                    ep = low[i]
                    af = acceleration
            else:
                if high[i] > psar[i]:
                    bull = True
                    reverse = True
                    psar[i] = ep
                    ep = high[i]
                    af = acceleration

            # If no reversal, update EP and AF
            if not reverse:
                if bull:
                    # Move SAR to not be higher than the lows of the previous two periods
                    psar[i] = min(psar[i], low[i - 1], low[i - 2])
                    if high[i] > ep:
                        ep = high[i]
                        af = min(af + acceleration, maximum)
                else:
                    # Move SAR to not be lower than the highs of the previous two periods
                    psar[i] = max(psar[i], high[i - 1], high[i - 2])
                    if low[i] < ep:
                        ep = low[i]
                        af = min(af + acceleration, maximum)
        return psar

    def generate_all_indicators(self) -> Optional[pd.DataFrame]:
        """
        Calculates all defined technical indicators using pandas and numpy,
        without TA-Lib.

        Returns:
            Optional[pd.DataFrame]: A copy of the DataFrame with all indicators, or None on failure.
        """
        if self.df_with_indicators.empty or not all(c in self.df_with_indicators.columns for c in ['open', 'high', 'low', 'close', 'volume']):
            logger.error("DataFrame is empty or missing required columns for indicator generation.")
            return None

        min_required_len = 200
        if len(self.df_with_indicators) < min_required_len:
             logger.warning(f"Data has {len(self.df_with_indicators)} rows, less than the {min_required_len} recommended for long-period indicators (e.g., SMA200).")

        df = self.df_with_indicators
        cl = df['close']
        hi = df['high']
        lo = df['low']
        vo = df['volume']
        
        try:
            # --- Trend Indicators ---
            df['SMA_20'] = cl.rolling(window=20).mean()
            df['SMA_50'] = cl.rolling(window=50).mean()
            df['SMA_200'] = cl.rolling(window=200).mean()
            df['EMA_12'] = cl.ewm(span=12, adjust=False).mean()
            df['EMA_26'] = cl.ewm(span=26, adjust=False).mean()
            df['EMA_50'] = cl.ewm(span=50, adjust=False).mean()
            
            # MACD
            macd_line = df['EMA_12'] - df['EMA_26']
            df['MACD_12_26_9'] = macd_line
            df['MACDs_12_26_9'] = macd_line.ewm(span=9, adjust=False).mean()
            df['MACDh_12_26_9'] = macd_line - df['MACDs_12_26_9']

            # True Range for ATR and ADX
            high_low = hi - lo
            high_close_prev = abs(hi - cl.shift(1))
            low_close_prev = abs(lo - cl.shift(1))
            tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1, skipna=False)
            
            # ADX, +DI, -DI
            atr_14 = tr.ewm(span=14, adjust=False).mean()
            high_diff = hi.diff()
            low_diff = lo.diff()
            plus_dm = ((high_diff > low_diff) & (high_diff > 0)).astype(float) * high_diff
            minus_dm = ((low_diff > high_diff) & (low_diff > 0)).astype(float) * low_diff
            plus_dm_smooth = plus_dm.ewm(span=14, adjust=False).mean()
            minus_dm_smooth = minus_dm.ewm(span=14, adjust=False).mean()
            df['DMP_14'] = 100 * (plus_dm_smooth / atr_14.replace(0, 1e-9))
            df['DMN_14'] = 100 * (minus_dm_smooth / atr_14.replace(0, 1e-9))
            di_sum = df['DMP_14'] + df['DMN_14']
            dx = 100 * (abs(df['DMP_14'] - df['DMN_14']) / di_sum.replace(0, 1e-9))
            df['ADX_14'] = dx.ewm(span=14, adjust=False).mean()

            # Aroon
            n_aroon = 14
            df['AROONU_14'] = hi.rolling(n_aroon + 1).apply(lambda x: 100 * np.argmax(x.to_numpy()) / n_aroon, raw=False)
            df['AROOND_14'] = lo.rolling(n_aroon + 1).apply(lambda x: 100 * np.argmin(x.to_numpy()) / n_aroon, raw=False)
            
            # Parabolic SAR
            df['SAR_0.02_0.2'] = self._calculate_sar(hi, lo)

            # --- Momentum Indicators ---
            # RSI
            delta = cl.diff()
            gain = delta.where(delta > 0, 0.0)
            loss = -delta.where(delta < 0, 0.0)
            avg_gain = gain.ewm(span=14, adjust=False).mean()
            avg_loss = loss.ewm(span=14, adjust=False).mean()
            rs = avg_gain / avg_loss.replace(0, 1e-9)
            df['RSI_14'] = 100 - (100 / (1 + rs))

            # Stochastic Oscillator
            low_14 = lo.rolling(window=14).min()
            high_14 = hi.rolling(window=14).max()
            fast_k = 100 * ((cl - low_14) / (high_14 - low_14).replace(0, 1e-9))
            df['STOCHk_14_3_3'] = fast_k.rolling(window=3).mean()
            df['STOCHd_14_3_3'] = df['STOCHk_14_3_3'].rolling(window=3).mean()

            # CCI
            tp = (hi + lo + cl) / 3
            sma_tp = tp.rolling(window=20).mean()
            mean_dev = (tp - sma_tp).abs().rolling(window=20).mean()
            df['CCI_20'] = (tp - sma_tp) / (0.015 * mean_dev.replace(0, 1e-9))

            # Momentum and Rate of Change
            df['MOM_10'] = cl.diff(10)
            df['ROC_12'] = cl.pct_change(12) * 100

            # --- Volatility Indicators ---
            # Bollinger Bands
            bbm = cl.rolling(window=20).mean()
            bbs = cl.rolling(window=20).std()
            df['BBM_20_2.0'] = bbm
            df['BBU_20_2.0'] = bbm + (bbs * 2)
            df['BBL_20_2.0'] = bbm - (bbs * 2)
            df['BBP_20_2.0'] = (cl - df['BBL_20_2.0']) / (df['BBU_20_2.0'] - df['BBL_20_2.0']).replace(0, 1e-9)

            # ATR (already calculated for ADX)
            df['ATR_14'] = atr_14
            
            # Keltner Channels
            kc_ema = cl.ewm(span=20, adjust=False).mean()
            kc_atr = tr.ewm(span=10, adjust=False).mean()
            df['KCBe_20_2'] = kc_ema
            df['KCUe_20_2'] = kc_ema + (kc_atr * 2)
            df['KCLe_20_2'] = kc_ema - (kc_atr * 2)
            df['KCP_20_10_2'] = (cl - df['KCLe_20_2']) / (df['KCUe_20_2'] - df['KCLe_20_2']).replace(0, 1e-9)
            
            # Donchian Channels
            df['DCU_20'] = hi.rolling(20).max()
            df['DCL_20'] = lo.rolling(20).min()

            # --- Volume Indicators ---
            # On-Balance Volume (OBV)
            df['OBV'] = np.where(cl > cl.shift(), vo, np.where(cl < cl.shift(), -vo, 0)).cumsum()
            
            # Money Flow Index (MFI)
            rmf = tp * vo
            tp_diff = tp.diff()
            pos_mf = np.where(tp_diff > 0, rmf, 0)
            neg_mf = np.where(tp_diff < 0, rmf, 0)
            pos_mf_sum = pd.Series(pos_mf, index=df.index).rolling(14).sum()
            neg_mf_sum = pd.Series(neg_mf, index=df.index).rolling(14).sum()
            mfi_ratio = pos_mf_sum / neg_mf_sum.replace(0, 1e-9)
            df['MFI_14'] = 100 - (100 / (1 + mfi_ratio))
            
            # Chaikin Money Flow (CMF)
            mfm = ((cl - lo) - (hi - cl)) / (hi - lo).replace(0, 1e-9)
            mfv = mfm * vo
            df['CMF_20'] = mfv.rolling(20).sum() / vo.rolling(20).sum().replace(0, 1e-9)
            
            # VWAP (resets daily)
            df['VWAP_D'] = df.groupby(df.index.date).apply(lambda x: (x['close'] * x['volume']).cumsum() / x['volume'].cumsum().replace(0, 1e-9)).reset_index(level=0, drop=True)

            # --- Final Calculations ---
            self._add_golden_death_crosses()
            num_gen_cols = len(self.df_with_indicators.columns) - len(self.df_ohlcv_original.columns)
            logger.info(f"Successfully generated TA indicators using pandas/numpy. Added {num_gen_cols} columns.")
            return self.df_with_indicators.copy()

        except Exception as e:
            logger.error(f"Error generating technical indicators with pandas/numpy: {e}", exc_info=True)
            return self.df_with_indicators.copy() if not self.df_with_indicators.empty else None

    def get_structured_summary(self) -> Optional[Dict[str, Any]]:
        """
        Generates a structured dictionary summarizing the technical analysis.
        This method is backward-compatible with the previous pandas-ta version.

        Returns:
            Optional[Dict[str, Any]]: A dictionary containing the technical summary,
                                      or None if data is insufficient.
        """
        if self.df_with_indicators.empty or len(self.df_with_indicators) < 2:
            logger.error("Cannot generate summary: indicator DataFrame is empty or has insufficient data.")
            return None

        latest = self.df_with_indicators.iloc[-1]
        close_price = latest.get('close')
        if pd.isna(close_price):
            logger.error("Cannot generate summary: latest close price is NaN.")
            return None

        # --- 1. Calculate Rule-Based Sentiment Score ---
        score = 0.0
        # RSI
        rsi = latest.get('RSI_14', 50)
        if rsi > 70: score -= 0.2
        elif rsi < 30: score += 0.2
        elif rsi > 55: score += 0.1
        elif rsi < 45: score -= 0.1
        
        # MACD
        macd_line = latest.get('MACD_12_26_9', 0)
        macd_signal = latest.get('MACDs_12_26_9', 0)
        if macd_line > macd_signal: score += 0.15
        else: score -= 0.15

        # Moving Averages
        sma50 = latest.get('SMA_50')
        sma200 = latest.get('SMA_200')
        if sma50 is not None and pd.notna(sma50) and close_price > sma50: score += 0.1
        if sma200 is not None and pd.notna(sma200) and close_price > sma200: score += 0.25
        if sma50 is not None and pd.notna(sma50) and close_price < sma50: score -= 0.1
        if sma200 is not None and pd.notna(sma200) and close_price < sma200: score -= 0.25

        # ADX
        adx = latest.get('ADX_14', 0)
        if adx > 25:
            dmp = latest.get('DMP_14', 0)
            dmn = latest.get('DMN_14', 0)
            if dmp > dmn: score += 0.15 # Strong uptrend
            else: score -= 0.15 # Strong downtrend
        
        # Golden/Death Cross in last 10 periods
        if latest.get('GOLDEN_X') == 1: score += 0.4
        elif self.df_with_indicators['GOLDEN_X'].tail(10).sum() > 0: score += 0.2
        if latest.get('DEATH_X') == 1: score -= 0.4
        elif self.df_with_indicators['DEATH_X'].tail(10).sum() > 0: score -= 0.2

        numeric_sentiment = np.clip(score, -1.0, 1.0)
        
        # --- 2. Determine Categorical Sentiment ---
        if numeric_sentiment > 0.4: sentiment_category = "Strong Bullish"
        elif numeric_sentiment > 0.15: sentiment_category = "Bullish"
        elif numeric_sentiment < -0.4: sentiment_category = "Strong Bearish"
        elif numeric_sentiment < -0.15: sentiment_category = "Bearish"
        else: sentiment_category = "Neutral"

        # --- 3. Construct Narrative and Cross Event Details ---
        narrative_parts = [f"Overall sentiment is {sentiment_category} (Score: {numeric_sentiment:.2f})."]
        cross_event_details = {"status": "N/A", "details": "SMA 50/200 not available."}

        if sma50 is not None and sma200 is not None and pd.notna(sma50) and pd.notna(sma200):
            if latest.get('GOLDEN_X') == 1:
                cross_event_details = {"status": "Golden Cross", "details": "A Golden Cross (SMA50 over SMA200) just occurred."}
                narrative_parts.append("Major bullish signal: Golden Cross confirmed.")
            elif latest.get('DEATH_X') == 1:
                cross_event_details = {"status": "Death Cross", "details": "A Death Cross (SMA50 under SMA200) just occurred."}
                narrative_parts.append("Major bearish signal: Death Cross confirmed.")
            elif sma50 > sma200:
                cross_event_details = {"status": "Bullish Alignment", "details": "SMA50 is currently above SMA200."}
            else:
                cross_event_details = {"status": "Bearish Alignment", "details": "SMA50 is currently below SMA200."}

        if pd.notna(rsi): narrative_parts.append(f"RSI at {rsi:.2f}.")
        if pd.notna(macd_line): narrative_parts.append(f"MACD line ({macd_line:.2f}) is {'above' if macd_line > macd_signal else 'below'} signal ({macd_signal:.2f}).")
        
        # --- 4. Build Structured Dictionary ---
        obv_value = latest.get('obv')
        summary = {
            "timestamp_utc": pd.Timestamp.now(tz='utc').isoformat(),
            "latest_candle_timestamp": latest.name.isoformat(),
            "close_price": close_price,
            "sentiment": {
                "category": sentiment_category,
                "numeric_score": round(numeric_sentiment, 3),
                "narrative": " ".join(narrative_parts)
            },
            "cross_event": cross_event_details,
            "key_indicators": {
                "rsi_14": round(latest.get('RSI_14', 0), 2),
                "macd_12_26_9": round(latest.get('MACD_12_26_9', 0), 4),
                "macds_12_26_9": round(latest.get('MACDs_12_26_9', 0), 4),
                "adx_14": round(latest.get('ADX_14', 0), 2),
                "stoch_k_14_3_3": round(latest.get('STOCHk_14_3_3', 0), 2),
                "atr_14": round(latest.get('ATR_14', 0), 4),
            },
            "price_vs_ma": {
                "vs_sma_20": "above" if close_price > latest.get('SMA_20', np.inf) else "below",
                "vs_sma_50": "above" if close_price > latest.get('SMA_50', np.inf) else "below",
                "vs_sma_200": "above" if close_price > latest.get('SMA_200', np.inf) else "below",
                "vs_ema_50": "above" if close_price > latest.get('EMA_50', np.inf) else "below",
            },
            "volatility": {
                "bbands_percentage": round(latest.get('BBP_20_2.0', 0), 3),
                "kc_percentage": round(latest.get('KCP_20_10_2', 0), 3),
            },
            "volume": {
                "obv": obv_value if pd.notna(obv_value) else None,
                "cmf_20": round(latest.get('CMF_20', 0), 3)
            }
        }
        return summary

# --- Main CLI for testing ---
async def main_cli():
    """Asynchronous command-line interface for testing."""
    print("--- Technical Analysis Module CLI Test (pandas/numpy Version) ---")

    if not _MarketResolver_imported:
        print("ERROR: MarketResolver is not available for CLI testing.")
        return

    asset_input = input("Enter coin name or symbol (e.g., SOL, BONK): ").strip() or "SOL"
    timeframe_input = input("Enter timeframe (e.g., 1h) [1h]: ").strip() or "1h"
    limit_input = int(input("Enter number of candles [300]: ").strip() or "300")

    df_ohlcv = None
    try:
        logger.info(f"Fetching OHLCV for {asset_input} via MarketResolver...")
        resolver = MarketResolver()
        # Using the bridge method you built into MarketResolver
        df_ohlcv = await resolver.fetch_ohlcv(
            symbol=asset_input,
            timeframe=timeframe_input,
            limit=limit_input
        )
    except Exception as e:
        logger.error(f"Error fetching market data via resolver: {e}", exc_info=True)

    if df_ohlcv is None or df_ohlcv.empty:
        print(f"\nCould not fetch OHLCV data for {asset_input}. Aborting.")
        return

    print(f"\nFetched {len(df_ohlcv)} candles for {asset_input}.")
    analyzer = TechnicalAnalyzer(df_ohlcv)
    
    df_indicators = analyzer.generate_all_indicators()
    
    if df_indicators is not None and not df_indicators.empty:
        print("\n--- DataFrame with Technical Indicators (Last 5 rows) ---")
        pd.set_option("display.max_columns", 15)
        pd.set_option("display.width", 160)
        # Display a subset of important columns for readability
        display_cols = [
            'close', 'SMA_50', 'SMA_200', 'RSI_14', 'MACD_12_26_9', 'MACDs_12_26_9',
            'ADX_14', 'BBP_20_2.0', 'ATR_14', 'OBV', 'CMF_20', 'GOLDEN_X', 'DEATH_X'
        ]
        existing_cols = [col for col in display_cols if col in df_indicators.columns]
        print(df_indicators[existing_cols].tail())

        print("\n--- Structured Technical Analysis Summary ---")
        structured_summary = analyzer.get_structured_summary()
        
        if structured_summary:
            import json
            print(json.dumps(structured_summary, indent=2))
        else:
            print("Failed to generate structured summary.")
    else:
        print("\nIndicator generation failed or resulted in an empty DataFrame.")


if __name__ == "__main__":
    if not logger.handlers:
         logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
         logger = logging.getLogger("TechnicalAnalysis_Main")

    try:
        asyncio.run(main_cli())
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
    except Exception as e:
        logger.critical(f"A critical error occurred in the CLI: {e}", exc_info=True)
        print(f"\nAn unexpected error occurred: {e}")