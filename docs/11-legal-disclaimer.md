# Legal Disclaimer

> **READ THIS ENTIRE DOCUMENT BEFORE USING THE POLYMARKET COPY TRADING BOT.**

---

## Not Financial Advice

The Polymarket Copy Trading Bot is **open-source software** provided for educational and experimental purposes. Nothing in this software, its documentation, its configuration defaults, or its output constitutes financial advice, investment advice, trading advice, or any other form of professional advice.

The bot does not:
- Analyze market fundamentals or technical indicators
- Predict market outcomes or price movements
- Recommend specific trades, positions, or markets
- Assess the risk-reward profile of any particular trade
- Provide tax, legal, or regulatory guidance

Any trading decisions you make are solely your own responsibility. You should consult with qualified financial, legal, and tax professionals before engaging in any trading activity.

---

## Risk of Loss

**Trading on prediction markets involves substantial risk of loss and is not suitable for all investors.** You may lose some or all of your invested capital.

Specific risks include, but are not limited to:

### Market Risk
Prediction market positions can resolve to zero. Unlike traditional investments where you may recover some value, a losing prediction market position can be completely worthless after resolution. The maximum loss on any single position is 100% of the invested amount.

### Leader Risk
The bot copies trades from other traders ("leaders"). These leaders:
- May have no actual investment skill
- May change their strategy without notice
- May experience periods of significant losses
- May be trading with different risk tolerances than you
- May have accounts that are no longer active
- May engage in manipulative or deceptive trading patterns

Past performance of any leader is **not indicative of future results**. A leader who was profitable historically may lose money in the future.

### Technical Risk
The bot is software, and all software contains bugs. Technical risks include:
- **Execution errors** — Orders may be submitted at incorrect prices or sizes due to bugs
- **Missed signals** — Leader trades may not be detected due to API issues or polling gaps
- **Failed exits** — Stop-loss or take-profit orders may not execute as expected
- **Data corruption** — Database errors may lead to incorrect position tracking
- **Network failures** — Internet or blockchain outages may prevent order submission or cancellation
- **Smart contract risks** — Interactions with on-chain contracts carry inherent risks including bugs, exploits, and governance changes

### Liquidity Risk
Polymarket markets may have limited liquidity. This means:
- You may not be able to enter or exit positions at desired prices
- Large orders may experience significant slippage
- The orderbook may be too thin for the bot's minimum order size
- Market makers may withdraw liquidity during volatile periods

### Counterparty Risk
Polymarket operates as a centralized order book and settlement system. Risks include:
- Exchange downtime or outages
- Changes to Polymarket's fee structure, trading rules, or API
- Regulatory actions that could restrict or shut down the platform
- Smart contract vulnerabilities in Polymarket's settlement layer

---

## No Guarantee of Profit

**The bot makes absolutely no guarantee of profit.** There is no trading system, algorithm, or strategy that can guarantee profits in prediction markets or any other financial market.

The bot's configuration defaults, risk parameters, and leader scoring weights are **not optimized for profit**. They are conservative safety defaults designed to limit losses. You must tune these parameters yourself based on your own risk tolerance and market understanding.

---

## Self-Hosted Software Liability

### "As Is" Basis

The software is provided **"as is"**, without warranty of any kind, express or implied, including but not limited to the warranties of merchantability, fitness for a particular purpose, and non-infringement. In no event shall the authors or copyright holders be liable for any claim, damages, or other liability, whether in an action of contract, tort, or otherwise, arising from, out of, or in connection with the software or the use or other dealings in the software.

### No Support Obligation

The maintainers of this software are not obligated to:
- Provide technical support
- Fix bugs in a timely manner
- Ensure compatibility with future Polymarket API changes
- Compensate you for any losses incurred through use of the software
- Monitor or supervise your use of the software

### Operator Responsibility

As the operator of this software, **you alone are responsible for**:
- Understanding how the software works before using it
- Configuring risk parameters appropriate for your financial situation
- Monitoring the bot's operation while it is running
- Ensuring your use complies with all applicable laws and regulations
- Maintaining the security of your private keys and credentials
- Paying any taxes owed on trading profits
- Managing any losses incurred through the bot's operation

---

## Compliance Responsibility

### Jurisdictional Compliance

You are solely responsible for determining whether your use of this software complies with the laws and regulations of your jurisdiction. This includes, but is not limited to:

- **Securities laws** — Prediction markets may be regulated as securities or derivatives in some jurisdictions
- **Gambling laws** — Prediction markets may be classified as gambling in some jurisdictions
- **Anti-money laundering (AML)** — Trading activity may be subject to AML reporting requirements
- **Tax obligations** — Profits from prediction market trading may be taxable income
- **Export controls** — Some jurisdictions restrict access to prediction market platforms

The bot's geoblock check is a **convenience feature**, not a legal compliance guarantee. It relies on Polymarket's API, which may be inaccurate, outdated, or unavailable. You must independently verify your legal right to trade.

### Polymarket Terms of Service

Your use of this software must comply with Polymarket's Terms of Service. The bot interacts with Polymarket's APIs and smart contracts, and any violation of their terms could result in:
- Account suspension or termination
- Loss of access to your positions and funds
- Legal action by Polymarket

---

## Jurisdiction Warnings

### United States
Polymarket restricts access from the United States for real-money trading. The CFTC has taken action against prediction market operators in the past. US residents should not use this software for real-money trading on Polymarket unless they have verified the legal status of doing so.

### European Union
Prediction markets may fall under MiFID II or national gambling regulations depending on the member state. EU residents should consult local regulations before trading.

### Other Jurisdictions
Many countries have specific regulations regarding online betting, derivatives trading, and cryptocurrency. It is your responsibility to understand and comply with your local laws.

---

## USE AT YOUR OWN RISK

By using the Polymarket Copy Trading Bot, you acknowledge and agree that:

1. You have read and understood this entire disclaimer
2. You understand the risks involved in prediction market trading
3. You are solely responsible for any financial losses incurred
4. The software is provided without any warranty or guarantee
5. The authors and maintainers are not liable for any damages
6. You will comply with all applicable laws and regulations
7. You will not hold the authors responsible for any trading outcomes
8. You understand that past performance does not guarantee future results
9. You accept that the software may contain bugs that could cause financial loss
10. You are using the software entirely at your own risk

**If you do not agree with any part of this disclaimer, do not use the software.**

---

## MIT License Notice

This software is released under the MIT License. The full license text is available in the repository's `LICENSE` file. The MIT License explicitly disclaims all warranties and limits liability to the maximum extent permitted by applicable law.

---

## Contact

If you have questions about this disclaimer, please open an issue on the project's GitHub repository. **Do not contact the maintainers for financial advice, trading recommendations, or regulatory guidance.**
