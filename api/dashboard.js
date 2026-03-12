const { getDashboardPayload } = require("../lib/dashboard");

module.exports = async (req, res) => {
  try {
    const payload = await getDashboardPayload({
      forceFresh: req.query?.fresh === "1",
    });

    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.status(200).json(payload);
  } catch (error) {
    res.setHeader("Content-Type", "application/json; charset=utf-8");
    res.setHeader("Cache-Control", "no-store");
    res.status(502).json({
      success: false,
      error: { message: `Dashboard build failed: ${error.message}` },
    });
  }
};
