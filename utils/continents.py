
africa = [
    'Algeria', 'Benin', 'Burkina Faso', 'Ghana', "Cote d'Ivoire", 'Nigeria', 'Niger',
    'Tunisia', 'Togo', 'South Africa', 'Botswana', 'Congo (Brazzaville)',
    'Congo (Kinshasa)', 'Swaziland', 'Central African Republic',
    'Equatorial Guinea', 'Saint Helena', 'Mauritius', 'British Indian Ocean Territory',
    'Cameroon', 'Zambia', 'Comoros', 'Mayotte', 'Reunion', 'Madagascar', 'Angola',
    'Gabon', 'Sao Tome and Principe', 'Mozambique', 'Seychelles', 'Chad',
    'Zimbabwe', 'Malawi', 'Lesotho', 'Mali', 'Gambia', 'Sierra Leone',
    'Guinea-Bissau', 'Liberia', 'Morocco', 'Senegal', 'Mauritania', 'Guinea',
    'Cape Verde', 'Ethiopia', 'Burundi', 'Somalia', 'Egypt', 'Kenya', 'Libya',
    'Rwanda', 'Sudan', 'South Sudan', 'Tanzania', 'Uganda', 'Namibia',
    'Djibouti', 'Eritrea', 'Western Sahara'
]
north_america = [
    'Canada', 'United States', 'Mexico', 'Greenland', 'Bermuda',
    'Saint Pierre and Miquelon', 'Cayman Islands', 'Bahamas', 'Belize',
    'Guatemala', 'Honduras', 'Nicaragua', 'Costa Rica', 'El Salvador', 'Panama',
    'Turks and Caicos Islands', 'Dominican Republic', 'Haiti', 'Cuba',
    'Jamaica', 'Puerto Rico', 'Virgin Islands', 'British Virgin Islands',
    'Montserrat', 'Saint Kitts and Nevis', 'Saint Lucia', 'Grenada',
    'Barbados', 'Trinidad and Tobago', 'Dominica', 'Saint Vincent and the Grenadines',
    'Antigua and Barbuda', 'Martinique', 'Guadeloupe', 'Aruba',
    'Netherlands Antilles', 'Anguilla'
]
south_america = [
    'Argentina', 'Brazil', 'Chile', 'Ecuador', 'Paraguay', 'Colombia',
    'Bolivia', 'Suriname', 'French Guiana', 'Peru', 'Uruguay', 'Venezuela', 'Guyana'
]
asia = [
    'Afghanistan', 'Armenia', 'Azerbaijan', 'Bahrain', 'Bangladesh', 'Bhutan',
    'Brunei', 'Burma', 'Cambodia', 'China', 'Cyprus', 'East Timor', 'Georgia',
    'Hong Kong', 'India', 'Indonesia', 'Iran', 'Iraq', 'Israel', 'Japan',
    'Jordan', 'Kazakhstan', 'Kuwait', 'Kyrgyzstan', 'Laos', 'Lebanon', 'Macau',
    'Malaysia', 'Maldives', 'Mongolia', 'Myanmar', 'Nepal', 'North Korea',
    'Oman', 'Pakistan', 'Palestine', 'Philippines', 'Qatar', 'Russia',
    'Saudi Arabia', 'Singapore', 'South Korea', 'Sri Lanka', 'Syria', 'Taiwan',
    'Tajikistan', 'Thailand', 'Turkey', 'Turkmenistan', 'United Arab Emirates',
    'Uzbekistan', 'Vietnam', 'West Bank', 'Yemen'
]
europe = [
    'Albania', 'Austria', 'Belgium', 'Bosnia and Herzegovina', 'Bulgaria',
    'Croatia', 'Czech Republic', 'Denmark', 'Estonia', 'Faroe Islands', 'Finland',
    'France', 'Germany', 'Gibraltar', 'Greece', 'Guernsey', 'Hungary', 'Iceland',
    'Ireland', 'Isle of Man', 'Italy', 'Jersey', 'Latvia', 'Lithuania',
    'Luxembourg', 'Macedonia', 'Malta', 'Moldova', 'Montenegro', 'Netherlands',
    'Norway', 'Poland', 'Portugal', 'Romania', 'Serbia', 'Slovakia',
    'Slovenia', 'Spain', 'Sweden', 'Switzerland', 'Ukraine', 'United Kingdom',
    'Svalbard'
]
oceania = [
    'Australia', 'New Zealand', 'Papua New Guinea', 'Fiji', 'Tonga', 'Kiribati',
    'Vanuatu', 'New Caledonia', 'Samoa', 'American Samoa', 'French Polynesia',
    'Wallis and Futuna', 'Cook Islands', 'Tuvalu', 'Micronesia', 'Palau',
    'Nauru', 'Solomon Islands', 'Marshall Islands', 'Guam', 'Northern Mariana Islands',
    'Midway Islands', 'Christmas Island', 'Norfolk Island', 'Niue',
    'Cocos (Keeling) Islands', 'Johnston Atoll', 'Wake Island'
]
antarctica = ['Antarctica']
def get_continent(country):
    if country in africa:
        return "Africa"
    elif country in europe:
        return "Europe"
    elif country in asia:
        return "Asia"
    elif country in north_america:
        return "North America"
    elif country in south_america:
        return "South America"
    elif country in oceania:
        return "Oceania"
    elif country in antarctica:
        return "Antarctica"
    else:
        return "Unknown"