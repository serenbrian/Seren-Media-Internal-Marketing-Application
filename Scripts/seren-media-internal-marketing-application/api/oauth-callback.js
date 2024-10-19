const axios = require('axios');

module.exports = async (req, res) => {
  const clientId = 'sq0idp-XJCGaN6csDILfjspDU5ZHg'; // Your Square Application ID
  const clientSecret = 'sq0csp-XCoa-b0eTwQINTL6FNo1JJzRbGEepfMG7OJ7591Puzs'; // Your Square Application Secret
  const redirectUri = 'https://your-vercel-app.vercel.app/api/oauth-callback'; // Change this to your Vercel app URL

  const authorizationCode = req.query.code;

  if (!authorizationCode) {
    return res.status(400).json({ message: 'Authorization code is missing' });
  }

  try {
    // Exchange the authorization code for access and refresh tokens
    const response = await axios.post('https://connect.squareup.com/oauth2/token', {
      client_id: clientId,
      client_secret: clientSecret,
      code: authorizationCode,
      grant_type: 'authorization_code',
      redirect_uri: redirectUri
    });

    const { access_token, refresh_token } = response.data;

    return res.status(200).json({
      message: 'OAuth process complete. Tokens received.',
      access_token: access_token,
      refresh_token: refresh_token
    });

  } catch (error) {
    console.error('Error exchanging authorization code:', error);
    return res.status(500).json({
      message: 'Error processing the OAuth callback',
      error: error.message
    });
  }
};